/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package objectstore opens the remote object stores described by
// ObjectStoreSpec as generic gocloud.dev buckets, resolving credentials the
// same way the Rust datastores do: from the referenced Secrets when set and
// from the ambient cloud identity otherwise.
package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/blob/s3blob"
	"gocloud.dev/gcerrors"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
	corev1 "k8s.io/api/core/v1"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
)

const gcsScope = "https://www.googleapis.com/auth/devstorage.read_write"

// SecretFunc resolves a Secret key reference to its value.
type SecretFunc func(ctx context.Context, ref *corev1.SecretKeySelector) ([]byte, error)

// Opener opens the bucket described by an ObjectStoreSpec.
type Opener func(ctx context.Context, spec telemetryv1alpha1.ObjectStoreSpec, secret SecretFunc) (*blob.Bucket, error)

// Open opens a remote object store. Local and in-memory stores have no
// bucket the operator can reach and are rejected.
func Open(ctx context.Context, spec telemetryv1alpha1.ObjectStoreSpec, secret SecretFunc) (*blob.Bucket, error) {
	switch {
	case spec.Type == telemetryv1alpha1.ObjectStoreAWS && spec.AWS != nil:
		return openS3(ctx, spec.AWS, secret)
	case spec.Type == telemetryv1alpha1.ObjectStoreAzure && spec.Azure != nil:
		return openAzure(ctx, spec.Azure, secret)
	case spec.Type == telemetryv1alpha1.ObjectStoreGCP && spec.GCP != nil:
		return openGCS(ctx, spec.GCP, secret)
	default:
		return nil, fmt.Errorf("object store type %q is not a configured remote object store", spec.Type)
	}
}

// Location renders the bucket and prefix as a URL for status messages.
func Location(spec telemetryv1alpha1.ObjectStoreSpec, prefix string) string {
	switch {
	case spec.AWS != nil:
		return "s3://" + spec.AWS.Bucket + "/" + prefix
	case spec.Azure != nil:
		return "azblob://" + spec.Azure.Account + "/" + spec.Azure.Container + "/" + prefix
	case spec.GCP != nil:
		return "gs://" + spec.GCP.Bucket + "/" + prefix
	default:
		return prefix
	}
}

// Prefix turns a subpath into a directory prefix, so "logs" matches
// "logs/..." but not "logs-archive/...".
func Prefix(subPath string) (string, error) {
	trimmed := strings.Trim(subPath, "/")
	if trimmed == "" {
		return "", errors.New("subPath must name a directory below the bucket root")
	}
	return trimmed + "/", nil
}

// CheckAccess lists at most one object under prefix, which proves the
// credentials are valid and the bucket exists and is listable.
func CheckAccess(ctx context.Context, bucket *blob.Bucket, prefix string) error {
	if _, _, err := bucket.ListPage(ctx, blob.FirstPageToken, 1, &blob.ListOptions{Prefix: prefix}); err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return fmt.Errorf("bucket not found: %w", err)
		}
		return fmt.Errorf("list %q: %w", prefix, err)
	}
	return nil
}

// DeletePrefix deletes every object under prefix with up to concurrency
// deletes in flight and returns how many it deleted. A missing bucket has
// nothing to delete. Objects that vanish concurrently are not errors.
func DeletePrefix(ctx context.Context, bucket *blob.Bucket, prefix string, concurrency int) (int64, error) {
	var deleted atomic.Int64
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(concurrency)
	iter := bucket.List(&blob.ListOptions{Prefix: prefix})
	var listErr error
	for {
		object, err := iter.Next(groupCtx)
		if err == io.EOF {
			break
		}
		if err != nil {
			if gcerrors.Code(err) != gcerrors.NotFound {
				listErr = fmt.Errorf("list %q: %w", prefix, err)
			}
			break
		}
		key := object.Key
		group.Go(func() error {
			if err := bucket.Delete(groupCtx, key); err != nil && gcerrors.Code(err) != gcerrors.NotFound {
				return fmt.Errorf("delete %q: %w", key, err)
			}
			deleted.Add(1)
			return nil
		})
	}
	err := group.Wait()
	return deleted.Load(), errors.Join(err, listErr)
}

func openS3(ctx context.Context, spec *telemetryv1alpha1.AWSObjectStoreSpec, secret SecretFunc) (*blob.Bucket, error) {
	endpoint, err := endpointURL(spec.Endpoint, spec.AllowHTTP)
	if err != nil {
		return nil, err
	}
	options := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(spec.Region)}
	if spec.AccessKeyIDSecretRef != nil && spec.SecretAccessKeySecretRef != nil {
		id, err := secretString(ctx, secret, spec.AccessKeyIDSecretRef)
		if err != nil {
			return nil, err
		}
		key, err := secretString(ctx, secret, spec.SecretAccessKeySecretRef)
		if err != nil {
			return nil, err
		}
		token := ""
		if spec.SessionTokenSecretRef != nil {
			if token, err = secretString(ctx, secret, spec.SessionTokenSecretRef); err != nil {
				return nil, err
			}
		}
		options = append(options, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(id, key, token)))
	}
	config, err := awsconfig.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}
	client := s3.NewFromConfig(config, func(o *s3.Options) {
		if endpoint == "" {
			return
		}
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = !spec.VirtualHostedStyle
		// Many S3-compatible stores reject the SDK's default CRC checksums.
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	return s3blob.OpenBucket(ctx, client, spec.Bucket, nil)
}

func openAzure(ctx context.Context, spec *telemetryv1alpha1.AzureObjectStoreSpec, secret SecretFunc) (*blob.Bucket, error) {
	serviceURL, err := endpointURL(spec.Endpoint, spec.AllowHTTP)
	if err != nil {
		return nil, err
	}
	if serviceURL == "" {
		serviceURL = fmt.Sprintf("https://%s.blob.core.windows.net", spec.Account)
	}
	containerURL := strings.TrimRight(serviceURL, "/") + "/" + url.PathEscape(spec.Container)
	options := &container.ClientOptions{ClientOptions: policy.ClientOptions{InsecureAllowCredentialWithHTTP: spec.AllowHTTP}}
	client, err := azureClient(ctx, spec, secret, containerURL, options)
	if err != nil {
		return nil, err
	}
	return azureblob.OpenBucket(ctx, client, nil)
}

func azureClient(ctx context.Context, spec *telemetryv1alpha1.AzureObjectStoreSpec, secret SecretFunc, containerURL string, options *container.ClientOptions) (*container.Client, error) {
	switch {
	case spec.AccessKeySecretRef != nil:
		key, err := secretString(ctx, secret, spec.AccessKeySecretRef)
		if err != nil {
			return nil, err
		}
		credential, err := container.NewSharedKeyCredential(spec.Account, key)
		if err != nil {
			return nil, fmt.Errorf("azure account key: %w", err)
		}
		return container.NewClientWithSharedKeyCredential(containerURL, credential, options)
	case spec.SASTokenSecretRef != nil:
		token, err := secretString(ctx, secret, spec.SASTokenSecretRef)
		if err != nil {
			return nil, err
		}
		return container.NewClientWithNoCredential(containerURL+"?"+strings.TrimPrefix(token, "?"), options)
	case spec.BearerTokenSecretRef != nil:
		token, err := secretString(ctx, secret, spec.BearerTokenSecretRef)
		if err != nil {
			return nil, err
		}
		return container.NewClient(containerURL, staticAzureToken(token), options)
	}
	var credential azcore.TokenCredential
	var err error
	switch {
	case spec.ClientSecret != nil:
		var clientSecret string
		if clientSecret, err = secretString(ctx, secret, &spec.ClientSecret.ClientSecretKeyRef); err != nil {
			return nil, err
		}
		credential, err = azidentity.NewClientSecretCredential(spec.ClientSecret.TenantID, spec.ClientSecret.ClientID, clientSecret, nil)
	case spec.WorkloadIdentity != nil:
		credential, err = azidentity.NewWorkloadIdentityCredential(&azidentity.WorkloadIdentityCredentialOptions{
			ClientID:      spec.WorkloadIdentity.ClientID,
			TenantID:      spec.WorkloadIdentity.TenantID,
			TokenFilePath: spec.WorkloadIdentity.TokenFile,
		})
	default:
		credential, err = azidentity.NewDefaultAzureCredential(nil)
	}
	if err != nil {
		return nil, fmt.Errorf("azure credential: %w", err)
	}
	return container.NewClient(containerURL, credential, options)
}

type staticAzureToken string

func (t staticAzureToken) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: string(t), ExpiresOn: time.Now().Add(time.Hour)}, nil
}

func openGCS(ctx context.Context, spec *telemetryv1alpha1.GCPObjectStoreSpec, secret SecretFunc) (*blob.Bucket, error) {
	client, err := gcsClient(ctx, spec, secret)
	if err != nil {
		return nil, err
	}
	options := &gcsblob.Options{}
	if spec.BaseURL != "" {
		options.ClientOptions = []option.ClientOption{option.WithEndpoint(strings.TrimRight(spec.BaseURL, "/") + "/storage/v1/")}
	}
	return gcsblob.OpenBucket(ctx, client, spec.Bucket, options)
}

func gcsClient(ctx context.Context, spec *telemetryv1alpha1.GCPObjectStoreSpec, secret SecretFunc) (*gcp.HTTPClient, error) {
	var tokens oauth2.TokenSource
	switch {
	case spec.ServiceAccountKeySecretRef != nil:
		key, err := secret(ctx, spec.ServiceAccountKeySecretRef)
		if err != nil {
			return nil, err
		}
		creds, err := google.CredentialsFromJSONWithType(ctx, key, google.ServiceAccount, gcsScope)
		if err != nil {
			return nil, fmt.Errorf("gcp service account key: %w", err)
		}
		tokens = creds.TokenSource
	case spec.BearerTokenSecretRef != nil:
		token, err := secretString(ctx, secret, spec.BearerTokenSecretRef)
		if err != nil {
			return nil, err
		}
		tokens = oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
	default:
		creds, err := gcp.DefaultCredentials(ctx)
		if err != nil {
			// Emulators behind a custom base URL usually need no credentials.
			if spec.BaseURL != "" {
				return gcp.NewAnonymousHTTPClient(gcp.DefaultTransport()), nil
			}
			return nil, fmt.Errorf("gcp default credentials: %w", err)
		}
		tokens = gcp.CredentialsTokenSource(creds)
	}
	return gcp.NewHTTPClient(gcp.DefaultTransport(), tokens)
}

// endpointURL validates a custom endpoint, which must be an absolute URL and
// may only use plain HTTP when allowHTTP is set.
func endpointURL(endpoint string, allowHTTP bool) (string, error) {
	if endpoint == "" {
		return "", nil
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" {
		return "", fmt.Errorf("endpoint %q must be an absolute http(s) URL", endpoint)
	}
	switch parsed.Scheme {
	case "https":
	case "http":
		if !allowHTTP {
			return "", fmt.Errorf("endpoint %q uses http; set allowHTTP to permit it", endpoint)
		}
	default:
		return "", fmt.Errorf("endpoint %q must be an absolute http(s) URL", endpoint)
	}
	return endpoint, nil
}

func secretString(ctx context.Context, secret SecretFunc, ref *corev1.SecretKeySelector) (string, error) {
	value, err := secret(ctx, ref)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(value)), nil
}
