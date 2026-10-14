package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	_ "github.com/Percona-Lab/percona-version-service/api"
	certmgrscheme "github.com/cert-manager/cert-manager/pkg/client/clientset/versioned/scheme"
	"github.com/go-logr/logr"
	"github.com/kelseyhightower/envconfig"
	"github.com/pkg/errors"
	uzap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsServer "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	ctrlWebhook "sigs.k8s.io/controller-runtime/pkg/webhook"

	"github.com/percona/percona-xtradb-cluster-operator/pkg/apis"
	pxcv1 "github.com/percona/percona-xtradb-cluster-operator/pkg/apis/pxc/v1"
	"github.com/percona/percona-xtradb-cluster-operator/pkg/controller"
	"github.com/percona/percona-xtradb-cluster-operator/pkg/features"
	"github.com/percona/percona-xtradb-cluster-operator/pkg/k8s"
	"github.com/percona/percona-xtradb-cluster-operator/pkg/version"
	"github.com/percona/percona-xtradb-cluster-operator/pkg/webhook"
)

var (
	GitCommit string
	GitBranch string
	BuildTime string
	scheme    = k8sruntime.NewScheme()
	setupLog  = ctrl.Log.WithName("setup")
)

// The cache checks its per-object settings against the scheme when the
// manager is created, so the types must be registered before that.
func init() {
	utilruntime.Must(addToScheme(scheme))
}

func main() {
	var metricsAddr string
	var probeAddr string

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")

	opts := zap.Options{
		Encoder: getLogEncoder(setupLog),
		Level:   getLogLevel(setupLog),
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	// The logger instantiated here can be changed to any logger
	// implementing the logr.Logger interface. This logger will
	// be propagated through the whole operator, generating
	// uniform and structured logs.
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	klog.SetLogger(ctrl.Log)

	sv, err := version.Server(context.Background())
	if err != nil {
		setupLog.Error(err, "unable to define server version")
		os.Exit(1)
	}
	setupLog.Info("Runs on", "platform", sv.Platform, "version", sv.Info)

	setupLog.Info("Manager starting up", "gitCommit", GitCommit, "gitBranch", GitBranch,
		"buildTime", BuildTime, "goVersion", runtime.Version(), "os", runtime.GOOS, "arch", runtime.GOARCH)

	namespace, err := k8s.GetWatchNamespace()
	if err != nil {
		setupLog.Error(err, "failed to get watch namespace")
		os.Exit(1)
	}
	operatorNamespace, err := k8s.GetOperatorNamespace()
	if err != nil {
		setupLog.Error(err, "failed to get operators' namespace")
		os.Exit(1)
	}

	envs := new(envConfig)
	if err := envconfig.Process("", envs); err != nil {
		setupLog.Error(err, "failed to parse env vars")
		os.Exit(1)
	}

	fg := features.NewGate()
	if err := fg.Set(envs.FeatureGates); err != nil {
		setupLog.Error(err, "failed to set feature gates")
		os.Exit(1)
	}
	fgCtx := features.NewContextWithGate(context.Background(), fg)
	setupLog.Info(
		"Feature gates",
		// These are set by the user
		"PXCO_FEATURE_GATES", features.ShowAssigned(fgCtx),
		// These are enabled, including features that are on by default
		"enabled", features.ShowEnabled(fgCtx),
	)

	options := ctrl.Options{
		Scheme: scheme,
		Metrics: metricsServer.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress: probeAddr,
		WebhookServer: ctrlWebhook.NewServer(ctrlWebhook.Options{
			Port: 9443,
		}),
		BaseContext: func() context.Context {
			return features.NewContextWithGate(context.Background(), fg)
		},
	}

	err = configureLeaderElection(&options, envs, operatorNamespace)
	if err != nil {
		setupLog.Error(err, "failed to configure leader election")
		os.Exit(1)
	}

	err = configureGroupKindConcurrency(&options, envs)
	if err != nil {
		setupLog.Error(err, "failed to configure group kind concurrency")
		os.Exit(1)
	}

	err = configureCache(&options, namespace, operatorNamespace, envs.SecretCacheExcludedTypes)
	if err != nil {
		setupLog.Error(err, "failed to configure cache")
		os.Exit(1)
	}

	// Get a config to talk to the apiserver
	config, err := ctrl.GetConfig()
	if err != nil {
		setupLog.Error(err, "")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(config, options)
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	setupLog.Info("Registering Components.")

	ctx := k8s.StartStopSignalHandler(mgr.GetClient(), strings.Split(namespace, ","))
	// Setup all Controllers
	if err := controller.AddToManager(ctx, mgr); err != nil {
		setupLog.Error(err, "")
		os.Exit(1)
	}

	if err := webhook.SetupWebhook(ctx, mgr); err != nil {
		setupLog.Error(err, "set up validation webhook")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}

	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	err = mgr.GetFieldIndexer().IndexField(
		ctx,
		&eventsv1.Event{},
		"regarding.name",
		func(rawObj client.Object) []string {
			event := rawObj.(*eventsv1.Event)
			return []string{event.Regarding.Name}
		},
	)
	if err != nil {
		setupLog.Error(err, "unable to index field")
		os.Exit(1)
	}

	setupLog.Info("Starting the Cmd.")

	// Start the Cmd
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "manager exited non-zero")
		os.Exit(1)
	}
}

func getLogEncoder(log logr.Logger) zapcore.Encoder {
	consoleEnc := zapcore.NewConsoleEncoder(uzap.NewDevelopmentEncoderConfig())

	s, found := os.LookupEnv("LOG_STRUCTURED")
	if !found {
		return consoleEnc
	}

	useJson, err := strconv.ParseBool(s)
	if err != nil {
		log.Info("Can't parse LOG_STRUCTURED env var, using console logger", "envVar", s)
		return consoleEnc
	}
	if !useJson {
		return consoleEnc
	}

	return zapcore.NewJSONEncoder(uzap.NewProductionEncoderConfig())
}

func getLogLevel(log logr.Logger) zapcore.LevelEnabler {
	l, found := os.LookupEnv("LOG_LEVEL")
	if !found {
		return zapcore.InfoLevel
	}

	switch strings.ToUpper(l) {
	case "VERBOSE", "DEBUG":
		return zapcore.DebugLevel
	case "INFO":
		return zapcore.InfoLevel
	case "ERROR":
		return zapcore.ErrorLevel
	default:
		log.Info("Unsupported log level", "level", l)
		return zapcore.InfoLevel
	}
}

const defaultElectionID = "08db1feb.percona.com"

type envConfig struct {
	LeaderElection   bool          `default:"true" envconfig:"PXCO_LEADER_ELECTION_ENABLED"`
	LeaderElectionID string        `envconfig:"PXCO_LEADER_ELECTION_LEASE_NAME"`
	LeaseDuration    time.Duration `default:"15s" envconfig:"PXCO_LEADER_ELECTION_LEASE_DURATION"`
	RenewDeadline    time.Duration `default:"10s" envconfig:"PXCO_LEADER_ELECTION_RENEW_DEADLINE"`
	RetryPeriod      time.Duration `default:"2s" envconfig:"PXCO_LEADER_ELECTION_RETRY_PERIOD"`

	FeatureGates string `envconfig:"PXCO_FEATURE_GATES"`

	Workers *int `envconfig:"MAX_CONCURRENT_RECONCILES"`

	SecretCacheExcludedTypes string `envconfig:"PXCO_SECRET_CACHE_EXCLUDED_TYPES"`
}

func configureLeaderElection(options *ctrl.Options, envs *envConfig, operatorNamespace string) error {
	options.LeaderElection = envs.LeaderElection
	if envs.LeaderElection {
		options.LeaderElectionID = defaultElectionID
	}

	options.LeaseDuration = &envs.LeaseDuration
	options.RenewDeadline = &envs.RenewDeadline
	options.RetryPeriod = &envs.RetryPeriod

	if lease := envs.LeaderElectionID; envs.LeaderElection && len(lease) > 0 {
		if errs := validation.IsDNS1123Subdomain(lease); len(errs) > 0 {
			return fmt.Errorf("value for PXCO_LEADER_ELECTION_LEASE_NAME is invalid: %v", errs)
		}
		options.LeaderElectionID = lease
		options.LeaderElectionNamespace = operatorNamespace
	}

	return nil
}

// addToScheme registers the Kubernetes, PXC and cert-manager types the
// operator uses.
func addToScheme(s *k8sruntime.Scheme) error {
	for _, add := range []func(*k8sruntime.Scheme) error{
		clientgoscheme.AddToScheme,
		apis.AddToScheme,
		certmgrscheme.AddToScheme,
	} {
		if err := add(s); err != nil {
			return err
		}
	}
	return nil
}

// operatorSecretTypes are the types of the Secrets the operator creates.
// Excluding them from the cache would make those Secrets look missing.
var operatorSecretTypes = map[corev1.SecretType]bool{
	corev1.SecretTypeOpaque: true,
	corev1.SecretTypeTLS:    true,
}

// configureCache limits the informer cache to the namespaces in
// WATCH_NAMESPACE, if set, and keeps Secrets of the types in
// PXCO_SECRET_CACHE_EXCLUDED_TYPES out of it. The PXC controller watches
// Secrets cluster-wide in cluster-wide mode, so without this the operator's
// memory grows with every Secret in the cluster, such as Helm release
// revisions (type helm.sh/release.v1), instead of with the Secrets it manages.
// excludedTypes is a comma-separated list of Secret types, for example
// "helm.sh/release.v1"; empty, the default, caches every Secret.
//
// The operator also reads Secrets that users reference by name, whatever
// their type. When types are excluded, the cache also holds the metadata of
// the Secrets of those types, so that it reports a referenced Secret of an
// excluded type as an error instead of NotFound.
func configureCache(options *ctrl.Options, watchNamespace, operatorNamespace, excludedTypes string) error {
	// Add support for MultiNamespace set in WATCH_NAMESPACE
	if len(watchNamespace) > 0 {
		namespaces := make(map[string]cache.Config)
		for _, ns := range append(strings.Split(watchNamespace, ","), operatorNamespace) {
			namespaces[ns] = cache.Config{}
		}
		options.Cache.DefaultNamespaces = namespaces
	}

	if strings.TrimSpace(excludedTypes) == "" {
		return nil
	}
	var excluded []corev1.SecretType
	var selectors []fields.Selector
	for _, entry := range strings.Split(excludedTypes, ",") {
		secretType := corev1.SecretType(strings.TrimSpace(entry))
		switch {
		case secretType == "" || strings.ContainsAny(string(secretType), " \t=!"):
			return errors.Errorf("value for PXCO_SECRET_CACHE_EXCLUDED_TYPES is invalid: %q is not a Secret type", entry)
		case operatorSecretTypes[secretType]:
			return errors.Errorf("value for PXCO_SECRET_CACHE_EXCLUDED_TYPES is invalid: the operator creates Secrets of type %q", secretType)
		case slices.Contains(excluded, secretType):
			continue
		}
		excluded = append(excluded, secretType)
		selectors = append(selectors, fields.OneTermNotEqualSelector("type", string(secretType)))
	}

	if options.Cache.ByObject == nil {
		options.Cache.ByObject = map[client.Object]cache.ByObject{}
	}
	options.Cache.ByObject[&corev1.Secret{}] = cache.ByObject{Field: fields.AndSelectors(selectors...)}
	options.NewCache = newExcludedSecretTypesCache(excluded)
	setupLog.Info("Secret types excluded from the cache", "types", excluded)
	return nil
}

// excludedSecretTypesCache explains cache misses caused by
// PXCO_SECRET_CACHE_EXCLUDED_TYPES. Next to the main cache it keeps one
// metadata-only cache per excluded type. When the main cache has no Secret
// for a key and the metadata cache of an excluded type has one, Get returns
// an error naming the setting instead of NotFound, without an API request.
type excludedSecretTypesCache struct {
	cache.Cache
	excluded map[corev1.SecretType]cache.Cache
}

func newExcludedSecretTypesCache(excludedTypes []corev1.SecretType) cache.NewCacheFunc {
	return func(config *rest.Config, options cache.Options) (cache.Cache, error) {
		mainCache, err := cache.New(config, options)
		if err != nil {
			return nil, err
		}

		excluded := make(map[corev1.SecretType]cache.Cache, len(excludedTypes))
		for _, secretType := range excludedTypes {
			metadataCache, err := cache.New(config, cache.Options{
				HTTPClient:           options.HTTPClient,
				Scheme:               options.Scheme,
				Mapper:               options.Mapper,
				DefaultNamespaces:    options.DefaultNamespaces,
				DefaultFieldSelector: fields.OneTermEqualSelector("type", string(secretType)),
				DefaultTransform:     keepNameOnly,
			})
			if err != nil {
				return nil, errors.Wrapf(err, "create metadata cache for Secret type %q", secretType)
			}
			// Register the informer now so that WaitForCacheSync waits for it.
			if _, err := metadataCache.GetInformer(context.Background(), secretMetadata(), cache.BlockUntilSynced(false)); err != nil {
				return nil, errors.Wrapf(err, "create metadata informer for Secret type %q", secretType)
			}
			excluded[secretType] = metadataCache
		}
		return &excludedSecretTypesCache{Cache: mainCache, excluded: excluded}, nil
	}
}

// keepNameOnly drops the metadata the existence check does not need.
// Annotations can be large: kubectl's last-applied-configuration holds the
// whole Secret, data included.
func keepNameOnly(in any) (any, error) {
	if obj, err := meta.Accessor(in); err == nil {
		obj.SetAnnotations(nil)
		obj.SetLabels(nil)
		obj.SetManagedFields(nil)
		obj.SetOwnerReferences(nil)
		obj.SetFinalizers(nil)
	}
	return in, nil
}

func secretMetadata() *metav1.PartialObjectMetadata {
	secret := &metav1.PartialObjectMetadata{}
	secret.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
	return secret
}

func (c *excludedSecretTypesCache) Start(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return c.Cache.Start(ctx) })
	for _, metadataCache := range c.excluded {
		g.Go(func() error { return metadataCache.Start(ctx) })
	}
	return g.Wait()
}

func (c *excludedSecretTypesCache) WaitForCacheSync(ctx context.Context) bool {
	for _, metadataCache := range c.excluded {
		if !metadataCache.WaitForCacheSync(ctx) {
			return false
		}
	}
	return c.Cache.WaitForCacheSync(ctx)
}

func (c *excludedSecretTypesCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	err := c.Cache.Get(ctx, key, obj, opts...)
	if _, isSecret := obj.(*corev1.Secret); !isSecret || !k8serrors.IsNotFound(err) {
		return err
	}

	for secretType, metadataCache := range c.excluded {
		metadataErr := metadataCache.Get(ctx, key, secretMetadata())
		switch {
		case metadataErr == nil:
			return errors.Errorf("secret %s has type %q, which PXCO_SECRET_CACHE_EXCLUDED_TYPES keeps out of the operator's cache: remove the type from the list", key, secretType)
		case !k8serrors.IsNotFound(metadataErr):
			return errors.Wrapf(metadataErr, "check whether secret %s has type %q", key, secretType)
		}
	}
	return err
}

func configureGroupKindConcurrency(options *ctrl.Options, envs *envConfig) error {
	groupKinds := []string{
		"PerconaXtraDBCluster." + pxcv1.SchemeGroupVersion.Group,
		"PerconaXtraDBClusterBackup." + pxcv1.SchemeGroupVersion.Group,
		"PerconaXtraDBClusterRestore." + pxcv1.SchemeGroupVersion.Group,
	}

	const defaultConcurrency = 1
	options.Controller.GroupKindConcurrency = make(map[string]int, len(groupKinds))
	for _, gk := range groupKinds {
		options.Controller.GroupKindConcurrency[gk] = defaultConcurrency
	}

	if envs.Workers != nil {
		if *envs.Workers <= 0 {
			return fmt.Errorf("MAX_CONCURRENT_RECONCILES must be a positive number: %d", *envs.Workers)
		}
		for _, gk := range groupKinds {
			options.Controller.GroupKindConcurrency[gk] = *envs.Workers
		}
	}
	return nil
}
