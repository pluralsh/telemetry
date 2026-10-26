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

package objectstore

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"gocloud.dev/blob"
	"gocloud.dev/blob/memblob"
	corev1 "k8s.io/api/core/v1"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
)

const (
	testBucket = "telemetry"
	testDir    = "logs"
	testPrefix = "logs/"
)

func TestPrefix(t *testing.T) {
	for input, want := range map[string]string{testDir: testPrefix, "/logs/": testPrefix, "tenants/a": "tenants/a/"} {
		got, err := Prefix(input)
		if err != nil || got != want {
			t.Errorf("Prefix(%q) = %q, %v; want %q", input, got, err, want)
		}
	}
	if _, err := Prefix("//"); err == nil {
		t.Error("Prefix(\"//\") succeeded; want an error for the bucket root")
	}
}

func TestEndpointURL(t *testing.T) {
	cases := []struct {
		endpoint  string
		allowHTTP bool
		wantErr   bool
	}{
		{endpoint: ""},
		{endpoint: "https://s3.example.com"},
		{endpoint: "http://minio:9000", allowHTTP: true},
		{endpoint: "http://minio:9000", wantErr: true},
		{endpoint: "minio:9000", allowHTTP: true, wantErr: true},
		{endpoint: "ftp://minio", allowHTTP: true, wantErr: true},
	}
	for _, c := range cases {
		_, err := endpointURL(c.endpoint, c.allowHTTP)
		if (err != nil) != c.wantErr {
			t.Errorf("endpointURL(%q, %v) error = %v; wantErr %v", c.endpoint, c.allowHTTP, err, c.wantErr)
		}
	}
}

func TestOpenRejectsLocalStores(t *testing.T) {
	if _, err := Open(context.Background(), telemetryv1alpha1.ObjectStoreSpec{Type: telemetryv1alpha1.ObjectStoreLocal}, nil); err == nil {
		t.Fatal("Open succeeded for a Local object store")
	}
}

func TestDeletePrefixOnlyDeletesTheDirectory(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer func() { _ = bucket.Close() }()
	keys := make([]string, 0, 206)
	keys = append(keys, "logs/manifest/1", "logs/compacted/a.sst", "logs/wal/2", "logs-archive/keep", "other/keep", testDir)
	for i := range 200 {
		keys = append(keys, fmt.Sprintf("logs/many/%03d", i))
	}
	for _, key := range keys {
		if err := bucket.WriteAll(ctx, key, []byte("x"), nil); err != nil {
			t.Fatal(err)
		}
	}

	deleted, err := DeletePrefix(ctx, bucket, "logs/", 8)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 203 {
		t.Errorf("deleted %d objects; want 203", deleted)
	}
	if remaining := listKeys(t, bucket); !slices.Equal(remaining, []string{testDir, "logs-archive/keep", "other/keep"}) {
		t.Errorf("remaining keys = %v", remaining)
	}
	if deleted, err = DeletePrefix(ctx, bucket, "logs/", 8); err != nil || deleted != 0 {
		t.Errorf("second pass deleted %d, %v; want 0, nil", deleted, err)
	}
	if err := CheckAccess(ctx, bucket, "logs/"); err != nil {
		t.Errorf("CheckAccess on an empty prefix: %v", err)
	}
}

func TestOpenS3CustomEndpoint(t *testing.T) {
	var (
		mu       sync.Mutex
		requests []*http.Request
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>telemetry</Name><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>`)
	}))
	defer server.Close()

	secrets := map[string]string{"id": "AKIDEXAMPLE", "key": "secret"}
	spec := telemetryv1alpha1.ObjectStoreSpec{Type: telemetryv1alpha1.ObjectStoreAWS, AWS: &telemetryv1alpha1.AWSObjectStoreSpec{
		Region: "us-east-1", Bucket: testBucket, Endpoint: server.URL, AllowHTTP: true,
		AccessKeyIDSecretRef:     &corev1.SecretKeySelector{Key: "id"},
		SecretAccessKeySecretRef: &corev1.SecretKeySelector{Key: "key"},
	}}
	bucket, err := Open(context.Background(), spec, func(_ context.Context, ref *corev1.SecretKeySelector) ([]byte, error) {
		return []byte(secrets[ref.Key] + "\n"), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bucket.Close() }()
	if err := CheckAccess(context.Background(), bucket, "logs/"); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(requests) == 0 {
		t.Fatal("no request reached the custom endpoint")
	}
	request := requests[0]
	if request.URL.Path != "/telemetry" || request.URL.Query().Get("prefix") != "logs/" {
		t.Errorf("request = %s; want a path-style list of /telemetry with prefix logs/", request.URL)
	}
	if auth := request.Header.Get("Authorization"); !strings.Contains(auth, "Credential=AKIDEXAMPLE/") {
		t.Errorf("Authorization = %q; want the static access key from the Secret", auth)
	}
}

func TestOpenGCSBaseURL(t *testing.T) {
	var path string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"kind":"storage#objects"}`)
	}))
	defer server.Close()

	spec := telemetryv1alpha1.ObjectStoreSpec{Type: telemetryv1alpha1.ObjectStoreGCP, GCP: &telemetryv1alpha1.GCPObjectStoreSpec{
		Bucket: testBucket, BaseURL: server.URL, BearerTokenSecretRef: &corev1.SecretKeySelector{Key: "token"},
	}}
	bucket, err := Open(context.Background(), spec, func(context.Context, *corev1.SecretKeySelector) ([]byte, error) {
		return []byte("token"), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bucket.Close() }()
	if err := CheckAccess(context.Background(), bucket, "logs/"); err != nil {
		t.Fatal(err)
	}
	if path != "/storage/v1/b/telemetry/o" {
		t.Errorf("request path = %q; want the JSON API under the custom base URL", path)
	}
}

func TestOpenAzureCustomEndpoint(t *testing.T) {
	var request *http.Request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request = r
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="utf-8"?><EnumerationResults><Blobs></Blobs><NextMarker/></EnumerationResults>`)
	}))
	defer server.Close()

	spec := telemetryv1alpha1.ObjectStoreSpec{Type: telemetryv1alpha1.ObjectStoreAzure, Azure: &telemetryv1alpha1.AzureObjectStoreSpec{
		Account: "devstoreaccount1", Container: testBucket, Endpoint: server.URL + "/devstoreaccount1", AllowHTTP: true,
		SASTokenSecretRef: &corev1.SecretKeySelector{Key: "sas"},
	}}
	bucket, err := Open(context.Background(), spec, func(context.Context, *corev1.SecretKeySelector) ([]byte, error) {
		return []byte("?sv=2024-01-01&sig=abc"), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bucket.Close() }()
	if err := CheckAccess(context.Background(), bucket, "logs/"); err != nil {
		t.Fatal(err)
	}
	if request == nil || request.URL.Path != "/devstoreaccount1/telemetry" || request.URL.Query().Get("sig") != "abc" || request.URL.Query().Get("prefix") != "logs/" {
		t.Errorf("request = %v; want a SAS-signed list of the container under the custom endpoint", request.URL)
	}
}

func listKeys(t *testing.T, bucket *blob.Bucket) []string {
	t.Helper()
	var keys []string
	iter := bucket.List(nil)
	for {
		object, err := iter.Next(context.Background())
		if err != nil {
			break
		}
		keys = append(keys, object.Key)
	}
	slices.Sort(keys)
	return keys
}
