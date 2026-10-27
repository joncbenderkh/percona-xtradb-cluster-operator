package pxc

import (
	"context"
	"testing"

	cm "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	certmgrscheme "github.com/cert-manager/cert-manager/pkg/client/clientset/versioned/scheme"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/percona/percona-xtradb-cluster-operator/pkg/apis"
	api "github.com/percona/percona-xtradb-cluster-operator/pkg/apis/pxc/v1"
	"github.com/percona/percona-xtradb-cluster-operator/pkg/naming"
	"github.com/percona/percona-xtradb-cluster-operator/pkg/pxc/users"
	"github.com/percona/percona-xtradb-cluster-operator/pkg/version"
)

// reconcilerFailingSecretGets returns a reconciler whose first `failures`
// Secret reads fail with a server error, as they can when Secrets are read
// from the API server.
func reconcilerFailingSecretGets(t *testing.T, failures int, objs ...client.Object) *ReconcilePerconaXtraDBCluster {
	return reconcilerFailingGets(t, failures, isSecret, objs...)
}

func isSecret(obj client.Object) bool      { _, ok := obj.(*corev1.Secret); return ok }
func isCertificate(obj client.Object) bool { _, ok := obj.(*cm.Certificate); return ok }
func isIssuer(obj client.Object) bool      { _, ok := obj.(*cm.Issuer); return ok }

// fakeReconciler returns a reconciler whose reads never fail.
func fakeReconciler(t *testing.T, objs ...client.Object) *ReconcilePerconaXtraDBCluster {
	return reconcilerWith(t, fullScheme(t), interceptor.Funcs{}, objs...)
}

// reconcilerFailingGets returns a reconciler whose first `failures` reads of
// the objects `match` selects fail with a server error.
func reconcilerFailingGets(t *testing.T, failures int, match func(client.Object) bool, objs ...client.Object) *ReconcilePerconaXtraDBCluster {
	return reconcilerWith(t, fullScheme(t), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if match(obj) && failures > 0 {
				failures--
				return k8serrors.NewServiceUnavailable("unavailable")
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}, objs...)
}

// fullScheme returns the scheme the operator runs with.
func fullScheme(t *testing.T) *runtime.Scheme {
	s := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(s))
	require.NoError(t, apis.AddToScheme(s))
	require.NoError(t, certmgrscheme.AddToScheme(s))
	return s
}

func reconcilerWith(t *testing.T, s *runtime.Scheme, funcs interceptor.Funcs, objs ...client.Object) *ReconcilePerconaXtraDBCluster {
	cl := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&api.PerconaXtraDBCluster{}).
		WithInterceptorFuncs(funcs).
		Build()
	return &ReconcilePerconaXtraDBCluster{
		recorder:      record.NewFakeRecorder(8),
		client:        cl,
		scheme:        cl.Scheme(),
		lockers:       newLockStore(),
		serverVersion: &version.ServerVersion{Platform: version.PlatformKubernetes},
	}
}

func TestDeleteCertsReturnsSecretReadErrors(t *testing.T) {
	cr := &api.PerconaXtraDBCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster1", Namespace: "pxc"}}
	tlsSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "cluster1-ssl", Namespace: "pxc"}}
	r := reconcilerFailingSecretGets(t, 1, tlsSecret)

	err := r.deleteCerts(t.Context(), cr)

	require.Error(t, err, "a Secret that could not be read must not be skipped")
	assert.True(t, k8serrors.IsServiceUnavailable(err))
	err = r.client.Get(t.Context(), client.ObjectKeyFromObject(tlsSecret), &corev1.Secret{})
	assert.True(t, k8serrors.IsNotFound(err), "the Secrets that could be read must be deleted, got %v", err)
}

func TestDeleteCertsReturnsCertificateReadErrors(t *testing.T) {
	cr := &api.PerconaXtraDBCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster1", Namespace: "pxc"}}
	cert := &cm.Certificate{ObjectMeta: metav1.ObjectMeta{Name: "cluster1-ssl", Namespace: "pxc"}}
	tlsSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "cluster1-ssl", Namespace: "pxc"}}
	r := reconcilerFailingGets(t, 1, isCertificate, cert, tlsSecret)

	err := r.deleteCerts(t.Context(), cr)

	require.Error(t, err, "a Certificate that could not be read must not be skipped: cert-manager would issue its Secret again")
	assert.True(t, k8serrors.IsServiceUnavailable(err))
	require.NoError(t, r.client.Get(t.Context(), client.ObjectKeyFromObject(cert), &cm.Certificate{}))
	require.NoError(t, r.client.Get(t.Context(), client.ObjectKeyFromObject(tlsSecret), &corev1.Secret{}),
		"the Secret must stay while its Certificate may still exist, or cert-manager issues it again on every attempt")

	require.NoError(t, r.deleteCerts(t.Context(), cr), "the next attempt must delete the rest")
	err = r.client.Get(t.Context(), client.ObjectKeyFromObject(cert), &cm.Certificate{})
	assert.True(t, k8serrors.IsNotFound(err), "got %v", err)
	err = r.client.Get(t.Context(), client.ObjectKeyFromObject(tlsSecret), &corev1.Secret{})
	assert.True(t, k8serrors.IsNotFound(err), "got %v", err)
}

func TestDeleteCertsReportsTheCertificateWhenBothReadsFail(t *testing.T) {
	cr := &api.PerconaXtraDBCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster1", Namespace: "pxc"}}
	issuer := &cm.Issuer{ObjectMeta: metav1.ObjectMeta{Name: "cluster1-pxc-ca-issuer", Namespace: "pxc"}}
	cert := &cm.Certificate{ObjectMeta: metav1.ObjectMeta{Name: "cluster1-ssl", Namespace: "pxc"}}
	isCertManagerObject := func(obj client.Object) bool { return isIssuer(obj) || isCertificate(obj) }
	// Both Issuer reads and the first Certificate read fail.
	r := reconcilerFailingGets(t, 3, isCertManagerObject, issuer, cert)

	err := r.deleteCerts(t.Context(), cr)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "get certificate cluster1-ssl", "the error must name the Certificate that keeps the Secrets from being deleted")
}

func TestDeleteCertsDeletesSecretsOnIssuerReadErrors(t *testing.T) {
	cr := &api.PerconaXtraDBCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster1", Namespace: "pxc"}}
	issuer := &cm.Issuer{ObjectMeta: metav1.ObjectMeta{Name: "cluster1-pxc-issuer", Namespace: "pxc"}}
	tlsSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "cluster1-ssl", Namespace: "pxc"}}
	r := reconcilerFailingGets(t, 1, isIssuer, issuer, tlsSecret)

	err := r.deleteCerts(t.Context(), cr)

	require.Error(t, err)
	assert.True(t, k8serrors.IsServiceUnavailable(err))
	err = r.client.Get(t.Context(), client.ObjectKeyFromObject(tlsSecret), &corev1.Secret{})
	assert.True(t, k8serrors.IsNotFound(err), "nothing issues the Secrets again while the Certificates are gone, so they must be deleted, got %v", err)
}

func TestDeleteCertsWithoutCertManagerAPI(t *testing.T) {
	cr := &api.PerconaXtraDBCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster1", Namespace: "pxc"}}
	tlsSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "cluster1-ssl", Namespace: "pxc"}}
	// Without the cert-manager CRDs the client's REST mapper finds no match
	// for its kinds; the types are always in the operator's scheme.
	r := reconcilerWith(t, fullScheme(t), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if gvks, _, err := c.Scheme().ObjectKinds(obj); err == nil && gvks[0].Group == cm.SchemeGroupVersion.Group {
				return &meta.NoKindMatchError{GroupKind: gvks[0].GroupKind()}
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}, tlsSecret)

	require.NoError(t, r.deleteCerts(t.Context(), cr), "Issuers and Certificates must be skipped on a cluster without cert-manager")
	err := r.client.Get(t.Context(), client.ObjectKeyFromObject(tlsSecret), &corev1.Secret{})
	assert.True(t, k8serrors.IsNotFound(err), "got %v", err)
}

func TestDeleteSSLFinalizerKeptOnSecretReadErrors(t *testing.T) {
	cr, err := readDefaultCR("cluster1", "pxc")
	require.NoError(t, err)
	cr.Finalizers = []string{naming.FinalizerDeleteSSL}
	cr.DeletionTimestamp = new(metav1.Now())
	tlsSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: cr.Name + "-ssl", Namespace: cr.Namespace}}
	r := reconcilerFailingSecretGets(t, 1, cr, tlsSecret)

	_, err = r.Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(cr)})

	require.NoError(t, err)
	stored := &api.PerconaXtraDBCluster{}
	require.NoError(t, r.client.Get(t.Context(), client.ObjectKeyFromObject(cr), stored))
	assert.Contains(t, stored.Finalizers, naming.FinalizerDeleteSSL, "the finalizer must stay until the TLS objects can be read")
	select {
	case event := <-r.recorder.(*record.FakeRecorder).Events:
		assert.Contains(t, event, naming.EventDeleteSSLFailed, "the cluster must carry an event explaining why it is not deleted")
	default:
		assert.Fail(t, "the cluster must carry an event explaining why it is not deleted")
	}

	_, err = r.Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(cr)})

	require.NoError(t, err)
	err = r.client.Get(t.Context(), client.ObjectKeyFromObject(cr), stored)
	assert.True(t, k8serrors.IsNotFound(err), "once the reads succeed the finalizer must go and the cluster with it, got %v", err)
}

// TestPasswordChangeChecksMySQLInitSecretFirst covers the handlers for
// MySQL without dual passwords. The dual-password handlers make the same
// call in the same place, but connect to the database before it, which a
// fake client cannot serve.
func TestPasswordChangeChecksMySQLInitSecretFirst(t *testing.T) {
	type handler func(r *ReconcilePerconaXtraDBCluster, ctx context.Context, cr *api.PerconaXtraDBCluster, secrets, internalSecrets *corev1.Secret) error
	withActions := func(h func(*ReconcilePerconaXtraDBCluster, context.Context, *api.PerconaXtraDBCluster, *corev1.Secret, *corev1.Secret, *userUpdateActions) error) handler {
		return func(r *ReconcilePerconaXtraDBCluster, ctx context.Context, cr *api.PerconaXtraDBCluster, secrets, internalSecrets *corev1.Secret) error {
			return h(r, ctx, cr, secrets, internalSecrets, &userUpdateActions{})
		}
	}
	handlers := map[string]handler{
		users.Root:        (*ReconcilePerconaXtraDBCluster).handleRootUserWithoutDP,
		users.Operator:    withActions((*ReconcilePerconaXtraDBCluster).handleOperatorUserWithoutDP),
		users.Monitor:     withActions((*ReconcilePerconaXtraDBCluster).handleMonitorUserWithoutDP),
		users.Xtrabackup:  withActions((*ReconcilePerconaXtraDBCluster).handleXtrabackupUserWithoutDP),
		users.Replication: withActions((*ReconcilePerconaXtraDBCluster).handleReplicationUserWithoutDP),
	}
	for user, handle := range handlers {
		t.Run(user, func(t *testing.T) {
			cr, err := readDefaultCR("cluster1", "pxc")
			require.NoError(t, err)
			cr.Status.Status = api.AppStateReady
			secrets := &corev1.Secret{Data: map[string][]byte{user: []byte("new")}}
			internalSecrets := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{"pass-expire-policy-for-1.13.0-user-" + user: "done"}},
				Data:       map[string][]byte{user: []byte("old")},
			}
			r := reconcilerFailingSecretGets(t, 1, mysqlInitSecretWith("ALTER USER 'monitor'@'%' IDENTIFIED BY 'p';\n"))

			err = handle(r, t.Context(), cr, secrets, internalSecrets)

			require.Error(t, err)
			assert.True(t, k8serrors.IsServiceUnavailable(err), "the init Secret must be checked before the password is changed, got %v", err)
		})
	}
}

func TestCheckMySQLInitSecretReturnsSecretReadErrors(t *testing.T) {
	cr := &api.PerconaXtraDBCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster1", Namespace: "pxc"}}
	r := reconcilerFailingSecretGets(t, 1, mysqlInitSecretWith("ALTER USER 'monitor'@'%' IDENTIFIED BY 'p';\n"))

	err := r.checkMySQLInitSecret(t.Context(), cr)

	require.Error(t, err, "the password must not be changed while the init file cannot be updated")
	assert.True(t, k8serrors.IsServiceUnavailable(err))
	assert.NoError(t, r.checkMySQLInitSecret(t.Context(), &api.PerconaXtraDBCluster{ObjectMeta: metav1.ObjectMeta{Name: "missing", Namespace: "pxc"}}))
}

func TestUpdateMySQLInitFile(t *testing.T) {
	cr := &api.PerconaXtraDBCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster1", Namespace: "pxc"}}
	root := &users.SysUser{Name: "root", Pass: "p", Hosts: []string{"%"}}

	t.Run("appends to the Secret", func(t *testing.T) {
		initSecret := mysqlInitSecretWith("ALTER USER 'monitor'@'%' IDENTIFIED BY 'p';\n")
		r := fakeReconciler(t, initSecret)

		require.NoError(t, r.checkMySQLInitSecret(t.Context(), cr))
		require.NoError(t, r.updateMySQLInitFile(t.Context(), cr, root))

		stored := &corev1.Secret{}
		require.NoError(t, r.client.Get(t.Context(), client.ObjectKeyFromObject(initSecret), stored))
		assert.Equal(t, "ALTER USER 'monitor'@'%' IDENTIFIED BY 'p';\nALTER USER 'root'@'%' IDENTIFIED BY 'p';\n", string(stored.Data["init.sql"]))
	})

	t.Run("starts over when a backup deleted the Secret", func(t *testing.T) {
		initSecret := mysqlInitSecretWith("ALTER USER 'monitor'@'%' IDENTIFIED BY 'old';\n")
		r := fakeReconciler(t, initSecret)

		require.NoError(t, r.checkMySQLInitSecret(t.Context(), cr))
		require.NoError(t, r.client.Delete(t.Context(), initSecret))
		require.NoError(t, r.updateMySQLInitFile(t.Context(), cr, root))

		stored := &corev1.Secret{}
		require.NoError(t, r.client.Get(t.Context(), client.ObjectKeyFromObject(initSecret), stored))
		assert.Equal(t, "SET SESSION wsrep_on=OFF;\nSET SESSION sql_log_bin=0;\nALTER USER 'root'@'%' IDENTIFIED BY 'p';\n", string(stored.Data["init.sql"]),
			"statements a backup discarded must not come back")
	})

	t.Run("starts over when a backup deleted the Secret after the read", func(t *testing.T) {
		initSecret := mysqlInitSecretWith("ALTER USER 'monitor'@'%' IDENTIFIED BY 'old';\n")
		r := fakeReconciler(t, initSecret)
		store := r.client.(client.WithWatch)
		// A backup deletes the Secret right after it has been read, and the
		// cache keeps serving the deleted Secret for a while.
		var stale *corev1.Secret
		r.client = interceptor.NewClient(store, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				secret, ok := obj.(*corev1.Secret)
				if ok && stale != nil {
					stale.DeepCopyInto(secret)
					return nil
				}
				err := c.Get(ctx, key, obj, opts...)
				if ok && err == nil {
					stale = secret.DeepCopy()
					require.NoError(t, c.Delete(ctx, secret.DeepCopy()))
				}
				return err
			},
		})

		require.NoError(t, r.updateMySQLInitFile(t.Context(), cr, root),
			"the write must not fail: a failed update runs the password change again")

		stored := &corev1.Secret{}
		require.NoError(t, store.Get(t.Context(), client.ObjectKeyFromObject(initSecret), stored))
		assert.Equal(t, "SET SESSION wsrep_on=OFF;\nSET SESSION sql_log_bin=0;\nALTER USER 'root'@'%' IDENTIFIED BY 'p';\n", string(stored.Data["init.sql"]),
			"statements a backup discarded must not come back")
	})

	t.Run("reads the Secret once", func(t *testing.T) {
		r := fakeReconciler(t, mysqlInitSecretWith("ALTER USER 'monitor'@'%' IDENTIFIED BY 'p';\n"))
		reads := 0
		r.client = interceptor.NewClient(r.client.(client.WithWatch), interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.Secret); ok {
					reads++
				}
				return c.Get(ctx, key, obj, opts...)
			},
		})

		require.NoError(t, r.updateMySQLInitFile(t.Context(), cr, root))

		assert.Equal(t, 1, reads, "a backup could delete the Secret between two reads, and the write would bring back what it discarded")
	})
}

func mysqlInitSecretWith(initSQL string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster1-mysql-init", Namespace: "pxc"},
		Data:       map[string][]byte{"init.sql": []byte(initSQL)},
	}
}
