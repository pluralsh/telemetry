package controller

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"sort"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	telemetryv1alpha1 "github.com/pluralsh/telemetry/go/operator/api/v1alpha1"
	productconfig "github.com/pluralsh/telemetry/go/operator/internal/config"
	"github.com/pluralsh/telemetry/go/operator/internal/resources"
)

// vectorSecrets are the Secrets a Vector's pods mount.
type vectorSecrets struct {
	config, token, tokenKey string
}

// reconcileInternalToken returns the Secret, key and value of the token
// replicas present on requests they forward to each other.
func (r *VectorReconciler) reconcileInternalToken(ctx context.Context, vector *telemetryv1alpha1.Vector) (string, string, []byte, error) {
	if ref := vector.Spec.Config.Auth.InternalTokenSecretRef; ref != nil {
		token, err := r.helper().secretValue(ctx, vector.Namespace, *ref)
		return ref.Name, ref.Key, token, err
	}
	generatedName := resources.Name(vector.Name, suffixInternalToken)
	generated := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: generatedName, Namespace: vector.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, generated, func() error {
		token := append([]byte(nil), generated.Data[resources.TokenKey]...)
		if len(token) == 0 {
			raw := make([]byte, 32)
			if _, err := rand.Read(raw); err != nil {
				return err
			}
			token = []byte(base64.RawURLEncoding.EncodeToString(raw))
		}
		generated.Labels, generated.Type, generated.Data = resources.Labels(vector, componentNone), corev1.SecretTypeOpaque, map[string][]byte{resources.TokenKey: token}
		return controllerutil.SetControllerReference(vector, generated, r.Scheme)
	})
	if err != nil {
		return "", "", nil, err
	}
	return generatedName, resources.TokenKey, append([]byte(nil), generated.Data[resources.TokenKey]...), nil
}

// resolveConfigInput resolves the credentials of the Vector and of the
// NamespaceAuthentications referencing it.
func (r *VectorReconciler) resolveConfigInput(ctx context.Context, vector *telemetryv1alpha1.Vector, token []byte) (productconfig.Input, error) {
	helper := r.helper()
	auths := &telemetryv1alpha1.NamespaceAuthenticationList{}
	if err := r.List(ctx, auths, client.InNamespace(vector.Namespace)); err != nil {
		return productconfig.Input{}, fmt.Errorf("list namespace authentications: %w", err)
	}
	items := lo.Filter(auths.Items, func(auth telemetryv1alpha1.NamespaceAuthentication, _ int) bool {
		return auth.Spec.DataStoreRef.Kind == dataStoreVector && auth.Spec.DataStoreRef.Name == vector.Name
	})
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	global, err := helper.resolveAccess(ctx, vector.Namespace, vector.Spec.Config.Auth.Global)
	if err != nil {
		return productconfig.Input{}, err
	}
	input := productconfig.Input{Vector: vector, Global: global, InternalToken: token}
	for _, auth := range items {
		password, err := helper.secretValue(ctx, vector.Namespace, auth.Spec.SecretKeyRef)
		if err != nil {
			return productconfig.Input{}, fmt.Errorf("resolve NamespaceAuthentication %s password: %w", auth.Name, err)
		}
		credential := productconfig.Credential{Username: auth.Spec.Username, Password: password, DataKey: "namespace-" + auth.Name + "-password"}
		access := productconfig.Access{}
		if auth.Spec.Permission == permissionRead {
			access.Read = []productconfig.Credential{credential}
		} else {
			access.Write = []productconfig.Credential{credential}
		}
		input.Namespaces = append(input.Namespaces, productconfig.NamespaceAccess{Name: auth.Spec.Namespace, KeyPrefix: auth.Name, Access: access})
	}
	if spec := vector.Spec.Config.Embedding; spec != nil && spec.APIKeySecretRef != nil {
		if input.EmbeddingAPIKey, err = helper.secretValue(ctx, vector.Namespace, *spec.APIKeySecretRef); err != nil {
			return productconfig.Input{}, fmt.Errorf("resolve embedding API key: %w", err)
		}
	}
	if spec := vector.Spec.Config.Auth.JWT; spec != nil {
		input.JWT = &productconfig.JWT{URL: spec.JWKS.URL, Issuer: spec.Issuer, Audience: spec.Audience, RefreshIntervalSeconds: spec.RefreshIntervalSeconds, RequestTimeoutSeconds: spec.RequestTimeoutSeconds}
		if spec.JWKS.SecretKeyRef != nil {
			if input.JWT.JWKS, err = helper.secretValue(ctx, vector.Namespace, *spec.JWKS.SecretKeyRef); err != nil {
				return productconfig.Input{}, err
			}
		}
	}
	return input, nil
}

func (r *VectorReconciler) helper() *MetricsReconciler {
	return &MetricsReconciler{Client: r.Client, Scheme: r.Scheme, DefaultProductVersion: r.DefaultProductVersion}
}

func vectorSecretNames(vector *telemetryv1alpha1.Vector) []string {
	auth := vector.Spec.Config.Auth
	names := lo.FilterMap(append(append([]telemetryv1alpha1.BasicCredentialSpec{}, auth.Global.Read...), auth.Global.Write...), func(spec telemetryv1alpha1.BasicCredentialSpec, _ int) (string, bool) {
		return spec.Password.Name, spec.Password.Name != ""
	})
	refs := []*corev1.SecretKeySelector{auth.InternalTokenSecretRef}
	if auth.JWT != nil {
		refs = append(refs, auth.JWT.JWKS.SecretKeyRef)
	}
	if embedding := vector.Spec.Config.Embedding; embedding != nil {
		refs = append(refs, embedding.APIKeySecretRef)
	}
	for _, ref := range refs {
		if ref != nil && ref.Name != "" {
			names = append(names, ref.Name)
		}
	}
	return lo.Uniq(names)
}

func vectorForNamespaceAuth(_ context.Context, obj client.Object) []reconcile.Request {
	auth := obj.(*telemetryv1alpha1.NamespaceAuthentication)
	if auth.Spec.DataStoreRef.Kind != dataStoreVector || auth.Spec.DataStoreRef.Name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: auth.Namespace, Name: auth.Spec.DataStoreRef.Name}}}
}

func (r *VectorReconciler) vectorForSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	vectors := &telemetryv1alpha1.VectorList{}
	if err := r.List(ctx, vectors, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	requests := lo.FilterMap(vectors.Items, func(vector telemetryv1alpha1.Vector, _ int) (reconcile.Request, bool) {
		return reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&vector)}, lo.Contains(vectorSecretNames(&vector), obj.GetName())
	})
	auths := &telemetryv1alpha1.NamespaceAuthenticationList{}
	if err := r.List(ctx, auths, client.InNamespace(obj.GetNamespace())); err == nil {
		for i := range auths.Items {
			if auths.Items[i].Spec.SecretKeyRef.Name == obj.GetName() {
				requests = append(requests, vectorForNamespaceAuth(ctx, &auths.Items[i])...)
			}
		}
	}
	return lo.UniqBy(requests, func(request reconcile.Request) types.NamespacedName { return request.NamespacedName })
}
