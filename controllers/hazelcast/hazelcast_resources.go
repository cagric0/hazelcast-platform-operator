package hazelcast

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/hazelcast/hazelcast-platform-operator/controllers/hazelcast/validation"
	"hash/crc32"
	"net"
	"strconv"

	"github.com/go-logr/logr"
	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	hazelcastv1alpha1 "github.com/hazelcast/hazelcast-platform-operator/api/v1alpha1"
	"github.com/hazelcast/hazelcast-platform-operator/internal/config"
	n "github.com/hazelcast/hazelcast-platform-operator/internal/naming"
	"github.com/hazelcast/hazelcast-platform-operator/internal/platform"
	"github.com/hazelcast/hazelcast-platform-operator/internal/util"
)

// Environment variables used for Hazelcast cluster configuration
const (
	// hzLicenseKey License key for Hazelcast cluster
	hzLicenseKey = "HZ_LICENSEKEY"
)

func (r *HazelcastReconciler) addFinalizer(ctx context.Context, h *hazelcastv1alpha1.Hazelcast, logger logr.Logger) error {
	if !controllerutil.ContainsFinalizer(h, n.Finalizer) && h.GetDeletionTimestamp() == nil {
		controllerutil.AddFinalizer(h, n.Finalizer)
		err := r.Update(ctx, h)
		if err != nil {
			return err
		}
		logger.V(util.DebugLevel).Info("Finalizer added into custom resource successfully")
	}
	return nil
}

func (r *HazelcastReconciler) executeFinalizer(ctx context.Context, h *hazelcastv1alpha1.Hazelcast, logger logr.Logger) error {
	if !controllerutil.ContainsFinalizer(h, n.Finalizer) {
		return nil
	}

	if err := r.removeClusterRole(ctx, h, logger); err != nil {
		return fmt.Errorf("ClusterRole could not be removed: %w", err)
	}
	if err := r.removeClusterRoleBinding(ctx, h, logger); err != nil {
		return fmt.Errorf("ClusterRoleBinding could not be removed: %w", err)
	}
	controllerutil.RemoveFinalizer(h, n.Finalizer)
	err := r.Update(ctx, h)
	if err != nil {
		return fmt.Errorf("failed to remove finalizer from custom resource: %w", err)
	}
	if util.IsPhoneHomeEnabled() {
		delete(r.metrics.HazelcastMetrics, h.UID)
	}
	ShutdownClient(ctx, types.NamespacedName{Name: h.Name, Namespace: h.Namespace})
	return nil
}

func (r *HazelcastReconciler) removeClusterRole(ctx context.Context, h *hazelcastv1alpha1.Hazelcast, logger logr.Logger) error {
	clusterRole := &rbacv1.ClusterRole{}
	err := r.Get(ctx, client.ObjectKey{Name: h.ClusterScopedName()}, clusterRole)
	if err != nil && errors.IsNotFound(err) {
		logger.V(util.DebugLevel).Info("ClusterRole is not created yet. Or it is already removed.")
		return nil
	}

	err = r.Delete(ctx, clusterRole)
	if err != nil {
		return fmt.Errorf("failed to clean up ClusterRole: %w", err)
	}
	logger.V(util.DebugLevel).Info("ClusterRole removed successfully")
	return nil
}

func (r *HazelcastReconciler) removeClusterRoleBinding(ctx context.Context, h *hazelcastv1alpha1.Hazelcast, logger logr.Logger) error {
	crb := &rbacv1.ClusterRoleBinding{}
	err := r.Get(ctx, client.ObjectKey{Name: h.ClusterScopedName()}, crb)
	if err != nil && errors.IsNotFound(err) {
		logger.V(util.DebugLevel).Info("ClusterRoleBinding is not created yet. Or it is already removed.")
		return nil
	}

	err = r.Delete(ctx, crb)
	if err != nil {
		return fmt.Errorf("failed to clean up ClusterRoleBinding: %w", err)
	}
	logger.V(util.DebugLevel).Info("ClusterRoleBinding removed successfully")
	return nil
}

func (r *HazelcastReconciler) reconcileClusterRole(ctx context.Context, h *hazelcastv1alpha1.Hazelcast, logger logr.Logger) error {

	clusterRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name:   h.ClusterScopedName(),
			Labels: labels(h),
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"endpoints", "pods", "nodes", "services", "secrets"},
				Verbs:     []string{"get", "list"},
			},
		},
	}

	if platform.GetType() == platform.OpenShift {
		clusterRole.Rules = append(clusterRole.Rules, rbacv1.PolicyRule{
			APIGroups: []string{"security.openshift.io"},
			Resources: []string{"securitycontextconstraints"},
			Verbs:     []string{"use"},
		},
		)
	}

	opResult, err := util.CreateOrUpdate(ctx, r.Client, clusterRole, func() error {
		return nil
	})
	if opResult != controllerutil.OperationResultNone {
		logger.Info("Operation result", "ClusterRole", h.ClusterScopedName(), "result", opResult)
	}
	return err
}

func (r *HazelcastReconciler) reconcileServiceAccount(ctx context.Context, h *hazelcastv1alpha1.Hazelcast, logger logr.Logger) error {
	serviceAccount := &corev1.ServiceAccount{
		ObjectMeta: metadata(h),
	}

	err := controllerutil.SetControllerReference(h, serviceAccount, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to set owner reference on ServiceAccount: %w", err)
	}

	opResult, err := util.CreateOrUpdate(ctx, r.Client, serviceAccount, func() error {
		return nil
	})
	if opResult != controllerutil.OperationResultNone {
		logger.Info("Operation result", "ServiceAccount", h.Name, "result", opResult)
	}
	return err
}

func (r *HazelcastReconciler) reconcileClusterRoleBinding(ctx context.Context, h *hazelcastv1alpha1.Hazelcast, logger logr.Logger) error {
	csName := h.ClusterScopedName()
	crb := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:   csName,
			Labels: labels(h),
		},
	}

	opResult, err := util.CreateOrUpdate(ctx, r.Client, crb, func() error {
		crb.Subjects = []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      h.Name,
				Namespace: h.Namespace,
			},
		}
		crb.RoleRef = rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     csName,
		}

		return nil
	})
	if opResult != controllerutil.OperationResultNone {
		logger.Info("Operation result", "ClusterRoleBinding", csName, "result", opResult)
	}
	return err
}

func (r *HazelcastReconciler) reconcileService(ctx context.Context, h *hazelcastv1alpha1.Hazelcast, logger logr.Logger) error {
	var service *corev1.Service
	if h.Spec.Persistence.IsExternal() {
		service = &corev1.Service{
			ObjectMeta: metadata(h),
			Spec: corev1.ServiceSpec{
				Selector: labels(h),
				Ports:    hazelcastAndAgentPort(),
			},
		}
	} else {
		service = &corev1.Service{
			ObjectMeta: metadata(h),
			Spec: corev1.ServiceSpec{
				Selector: labels(h),
				Ports:    hazelcastPort(),
			},
		}
	}

	err := controllerutil.SetControllerReference(h, service, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to set owner reference on Service: %w", err)
	}

	opResult, err := util.CreateOrUpdate(ctx, r.Client, service, func() error {
		service.Spec.Type = serviceType(h)
		if serviceType(h) == corev1.ServiceTypeClusterIP {
			// dirty hack to prevent the error when changing the service type
			service.Spec.Ports[0].NodePort = 0
		}

		return nil
	})
	if opResult != controllerutil.OperationResultNone {
		logger.Info("Operation result", "Service", h.Name, "result", opResult)
	}
	return err
}

func serviceType(h *hazelcastv1alpha1.Hazelcast) v1.ServiceType {
	if h.Spec.ExposeExternally.IsEnabled() {
		return h.Spec.ExposeExternally.DiscoveryK8ServiceType()
	}
	return corev1.ServiceTypeClusterIP
}

func (r *HazelcastReconciler) reconcileServicePerPod(ctx context.Context, h *hazelcastv1alpha1.Hazelcast, logger logr.Logger) error {
	if !h.Spec.ExposeExternally.IsSmart() {
		// Service per pod applies only to Smart type
		return nil
	}

	for i := 0; i < int(*h.Spec.ClusterSize); i++ {
		service := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      servicePerPodName(i, h),
				Namespace: h.Namespace,
				Labels:    servicePerPodLabels(h),
			},
			Spec: corev1.ServiceSpec{
				Selector:                 servicePerPodSelector(i, h),
				Ports:                    hazelcastPort(),
				PublishNotReadyAddresses: true,
			},
		}

		err := controllerutil.SetControllerReference(h, service, r.Scheme)
		if err != nil {
			return err
		}

		opResult, err := util.CreateOrUpdate(ctx, r.Client, service, func() error {
			service.Spec.Type = h.Spec.ExposeExternally.MemberAccessServiceType()
			return nil
		})

		if opResult != controllerutil.OperationResultNone {
			logger.Info("Operation result", "Service", servicePerPodName(i, h), "result", opResult)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *HazelcastReconciler) reconcileUnusedServicePerPod(ctx context.Context, h *hazelcastv1alpha1.Hazelcast) error {
	var s int
	if h.Spec.ExposeExternally.IsSmart() {
		s = int(*h.Spec.ClusterSize)
	}

	// Delete unused services (when the cluster was scaled down)
	// The current number of service per pod is always stored in the StatefulSet annotations
	sts := &appsv1.StatefulSet{}
	err := r.Client.Get(ctx, client.ObjectKey{Name: h.Name, Namespace: h.Namespace}, sts)
	if err != nil {
		if errors.IsNotFound(err) {
			// Not found, StatefulSet is not created yet, no need to delete any services
			return nil
		}
		return err
	}
	p, err := strconv.Atoi(sts.ObjectMeta.Annotations[n.ServicePerPodCountAnnotation])
	if err != nil {
		// Annotation not found, no need to delete any services
		return nil
	}

	for i := s; i < p; i++ {
		s := &v1.Service{}
		err := r.Client.Get(ctx, client.ObjectKey{Name: servicePerPodName(i, h), Namespace: h.Namespace}, s)
		if err != nil {
			if errors.IsNotFound(err) {
				// Not found, no need to remove the service
				continue
			}
			return err
		}
		err = r.Client.Delete(ctx, s)
		if err != nil {
			if errors.IsNotFound(err) {
				// Not found, no need to remove the service
				continue
			}
			return err
		}
	}

	return nil
}

func servicePerPodName(i int, h *hazelcastv1alpha1.Hazelcast) string {
	return fmt.Sprintf("%s-%d", h.Name, i)
}

func servicePerPodSelector(i int, h *hazelcastv1alpha1.Hazelcast) map[string]string {
	ls := labels(h)
	ls[n.PodNameLabel] = servicePerPodName(i, h)
	return ls
}

func servicePerPodLabels(h *hazelcastv1alpha1.Hazelcast) map[string]string {
	ls := labels(h)
	ls[n.ServicePerPodLabelName] = n.LabelValueTrue
	return ls
}

func hazelcastPort() []v1.ServicePort {
	return []corev1.ServicePort{
		{
			Name:       n.HazelcastPortName,
			Protocol:   corev1.ProtocolTCP,
			Port:       n.DefaultHzPort,
			TargetPort: intstr.FromString(n.Hazelcast),
		},
	}
}

func hazelcastAndAgentPort() []v1.ServicePort {
	return []corev1.ServicePort{
		{
			Name:       n.HazelcastPortName,
			Protocol:   corev1.ProtocolTCP,
			Port:       n.DefaultHzPort,
			TargetPort: intstr.FromString(n.Hazelcast),
		},
		{
			Name:       n.BackupAgentPortName,
			Protocol:   corev1.ProtocolTCP,
			Port:       n.DefaultAgentPort,
			TargetPort: intstr.FromString(n.BackupAgent),
		},
	}
}

func (r *HazelcastReconciler) isServicePerPodReady(ctx context.Context, h *hazelcastv1alpha1.Hazelcast) bool {
	if !h.Spec.ExposeExternally.IsSmart() {
		// Service per pod applies only to Smart type
		return true
	}

	// Check if each service per pod is ready
	for i := 0; i < int(*h.Spec.ClusterSize); i++ {
		s := &v1.Service{}
		err := r.Client.Get(ctx, client.ObjectKey{Name: servicePerPodName(i, h), Namespace: h.Namespace}, s)
		if err != nil {
			// Service is not created yet
			return false
		}
		if s.Spec.Type == v1.ServiceTypeLoadBalancer {
			if len(s.Status.LoadBalancer.Ingress) == 0 {
				// LoadBalancer service waiting for External IP to get assigned
				return false
			}
			for _, ingress := range s.Status.LoadBalancer.Ingress {
				// Hostname is set for load-balancer ingress points that are DNS based
				// (typically AWS load-balancers)
				if ingress.Hostname != "" {
					if _, err := net.DefaultResolver.LookupHost(ctx, ingress.Hostname); err != nil {
						// Hostname does not resolve yet
						return false
					}
				}
			}
		}
	}

	return true
}

func (r *HazelcastReconciler) reconcileConfigMap(ctx context.Context, h *hazelcastv1alpha1.Hazelcast, logger logr.Logger) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metadata(h),
	}

	err := controllerutil.SetControllerReference(h, cm, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to set owner reference on ConfigMap: %w", err)
	}

	opResult, err := util.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Data, err = hazelcastConfigMapData(r.Client, ctx, h)
		return err
	})
	if opResult != controllerutil.OperationResultNone {
		logger.Info("Operation result", "ConfigMap", h.Name, "result", opResult)
	}
	return err
}

func hazelcastConfigMapData(c client.Client, ctx context.Context, h *hazelcastv1alpha1.Hazelcast) (map[string]string, error) {
	mapList := &hazelcastv1alpha1.MapList{}
	err := c.List(ctx, mapList, client.MatchingFields{"hazelcastResourceName": h.Name})
	if err != nil {
		return nil, err
	}
	ml := filterPersistedMaps(mapList.Items)

	cfg := hazelcastConfigMapStruct(h)
	fillHazelcastConfigWithMaps(&cfg, ml)

	yml, err := yaml.Marshal(config.HazelcastWrapper{Hazelcast: cfg})
	if err != nil {
		return nil, err
	}
	return map[string]string{"hazelcast.yaml": string(yml)}, nil
}

func filterPersistedMaps(ml []hazelcastv1alpha1.Map) []hazelcastv1alpha1.Map {
	l := make([]hazelcastv1alpha1.Map, 0)

	for _, mp := range ml {
		switch mp.Status.State {
		case hazelcastv1alpha1.MapPersisting, hazelcastv1alpha1.MapSuccess:
			l = append(l, mp)
		case hazelcastv1alpha1.MapFailed, hazelcastv1alpha1.MapPending:
			if spec, ok := mp.Annotations[n.LastSuccessfulSpecAnnotation]; ok {
				ms := &hazelcastv1alpha1.MapSpec{}
				err := json.Unmarshal([]byte(spec), ms)
				if err != nil {
					continue
				}
				mp.Spec = *ms
				l = append(l, mp)
			}
		default:
		}
	}
	return l
}

func hazelcastConfigMapStruct(h *hazelcastv1alpha1.Hazelcast) config.Hazelcast {
	cfg := config.Hazelcast{
		Jet: config.Jet{
			Enabled: &[]bool{true}[0],
		},
		Network: config.Network{
			Join: config.Join{
				Kubernetes: config.Kubernetes{
					Enabled:     &[]bool{true}[0],
					ServiceName: h.Name,
				},
			},
			RestAPI: config.RestAPI{
				Enabled: &[]bool{true}[0],
				EndpointGroups: config.EndpointGroups{
					HealthCheck: config.EndpointGroup{
						Enabled: &[]bool{true}[0],
					},
					ClusterWrite: config.EndpointGroup{
						Enabled: &[]bool{true}[0],
					},
					Persistence: config.EndpointGroup{
						Enabled: &[]bool{true}[0],
					},
				},
			},
		},
	}

	if h.Spec.ExposeExternally.UsesNodeName() {
		cfg.Network.Join.Kubernetes.UseNodeNameAsExternalAddress = &[]bool{true}[0]
	}

	if h.Spec.ExposeExternally.IsSmart() {
		cfg.Network.Join.Kubernetes.ServicePerPodLabelName = n.ServicePerPodLabelName
		cfg.Network.Join.Kubernetes.ServicePerPodLabelValue = n.LabelValueTrue
	}

	if h.Spec.ClusterName != "" {
		cfg.ClusterName = h.Spec.ClusterName
	}

	if h.Spec.Persistence.IsEnabled() {
		cfg.Persistence = config.Persistence{
			Enabled:                   &[]bool{true}[0],
			BaseDir:                   h.Spec.Persistence.BaseDir,
			BackupDir:                 h.Spec.Persistence.BaseDir + "/hot-backup",
			Parallelism:               1,
			ValidationTimeoutSec:      120,
			DataLoadTimeoutSec:        900,
			ClusterDataRecoveryPolicy: clusterDataRecoveryPolicy(h.Spec.Persistence.ClusterDataRecoveryPolicy),
			AutoRemoveStaleData:       &[]bool{h.Spec.Persistence.AutoRemoveStaleData()}[0],
		}
		if h.Spec.Persistence.DataRecoveryTimeout != 0 {
			cfg.Persistence.ValidationTimeoutSec = h.Spec.Persistence.DataRecoveryTimeout
			cfg.Persistence.DataLoadTimeoutSec = h.Spec.Persistence.DataRecoveryTimeout
		}
	}
	return cfg
}

func clusterDataRecoveryPolicy(policyType hazelcastv1alpha1.DataRecoveryPolicyType) string {
	switch policyType {
	case hazelcastv1alpha1.FullRecovery:
		return "FULL_RECOVERY_ONLY"
	case hazelcastv1alpha1.MostRecent:
		return "PARTIAL_RECOVERY_MOST_RECENT"
	case hazelcastv1alpha1.MostComplete:
		return "PARTIAL_RECOVERY_MOST_COMPLETE"
	}
	return "FULL_RECOVERY_ONLY"
}

func fillHazelcastConfigWithMaps(cfg *config.Hazelcast, ml []hazelcastv1alpha1.Map) {
	if len(ml) != 0 {
		cfg.Map = map[string]config.Map{}
		for _, mcfg := range ml {
			cfg.Map[mcfg.MapName()] = createMapConfig(&mcfg.Spec)
		}
	}
}

func createMapConfig(ms *hazelcastv1alpha1.MapSpec) config.Map {
	m := config.Map{
		BackupCount:       *ms.BackupCount,
		AsyncBackupCount:  int32(0),
		TimeToLiveSeconds: *ms.TimeToLiveSeconds,
		ReadBackupData:    false,
		Eviction: config.MapEviction{
			Size:           *ms.Eviction.MaxSize,
			MaxSizePolicy:  string(ms.Eviction.MaxSizePolicy),
			EvictionPolicy: string(ms.Eviction.EvictionPolicy),
		},
		InMemoryFormat:    "BINARY",
		Indexes:           copyMapIndexes(ms.Indexes),
		StatisticsEnabled: true,
		HotRestart: config.MapHotRestart{
			Enabled: ms.PersistenceEnabled,
			Fsync:   false,
		},
	}
	return m
}

func copyMapIndexes(idx []hazelcastv1alpha1.IndexConfig) []config.MapIndex {
	ics := make([]config.MapIndex, len(idx))
	for i, index := range idx {
		ics[i].Type = string(index.Type)
		ics[i].Attributes = index.Attributes
		ics[i].Name = index.Name
		if index.BitmapIndexOptions != nil {
			ics[i].BitmapIndexOptions.UniqueKey = index.BitmapIndexOptions.UniqueKey
			ics[i].BitmapIndexOptions.UniqueKeyTransformation = string(index.BitmapIndexOptions.UniqueKeyTransition)
		}
	}

	return ics
}

func (r *HazelcastReconciler) reconcileStatefulset(ctx context.Context, h *hazelcastv1alpha1.Hazelcast, logger logr.Logger) error {
	ls := labels(h)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metadata(h),
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: ls,
			},
			ServiceName: h.Name,
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: ls,
				},
				Spec: v1.PodSpec{
					ServiceAccountName: h.Name,
					SecurityContext: &v1.PodSecurityContext{
						FSGroup:      &[]int64{65534}[0],
						RunAsNonRoot: &[]bool{true}[0],
						RunAsUser:    &[]int64{65534}[0],
					},
					Containers: []v1.Container{{
						Name: n.Hazelcast,
						Ports: []v1.ContainerPort{{
							ContainerPort: n.DefaultHzPort,
							Name:          n.Hazelcast,
							Protocol:      v1.ProtocolTCP,
						}},
						LivenessProbe: &v1.Probe{
							Handler: v1.Handler{
								HTTPGet: &v1.HTTPGetAction{
									Path:   "/hazelcast/health/node-state",
									Port:   intstr.FromInt(n.DefaultHzPort),
									Scheme: corev1.URISchemeHTTP,
								},
							},
							InitialDelaySeconds: 0,
							TimeoutSeconds:      10,
							PeriodSeconds:       10,
							SuccessThreshold:    1,
							FailureThreshold:    10,
						},
						ReadinessProbe: &v1.Probe{
							Handler: v1.Handler{
								HTTPGet: &v1.HTTPGetAction{
									Path:   "/hazelcast/health/node-state",
									Port:   intstr.FromInt(n.DefaultHzPort),
									Scheme: corev1.URISchemeHTTP,
								},
							},
							InitialDelaySeconds: 0,
							TimeoutSeconds:      10,
							PeriodSeconds:       10,
							SuccessThreshold:    1,
							FailureThreshold:    10,
						},
						SecurityContext: &v1.SecurityContext{
							RunAsNonRoot:             &[]bool{true}[0],
							RunAsUser:                &[]int64{65534}[0],
							Privileged:               &[]bool{false}[0],
							ReadOnlyRootFilesystem:   &[]bool{!h.Spec.Persistence.IsEnabled()}[0],
							AllowPrivilegeEscalation: &[]bool{false}[0],
							Capabilities: &v1.Capabilities{
								Drop: []v1.Capability{"ALL"},
							},
						},
						VolumeMounts: volumeMount(h),
					}},
					TerminationGracePeriodSeconds: &[]int64{600}[0],
					Volumes:                       volumes(h),
				},
			},
		},
	}

	if h.Spec.Persistence.IsEnabled() {
		if h.Spec.Persistence.UseHostPath() {
			sts.Spec.Template.Spec.Volumes = append(sts.Spec.Template.Spec.Volumes, hostPathVolume(h))
			sts.Spec.Template.Spec.Containers[0].SecurityContext.RunAsNonRoot = &[]bool{false}[0]
			sts.Spec.Template.Spec.Containers[0].SecurityContext.RunAsUser = &[]int64{0}[0]
			if platform.GetType() == platform.OpenShift {
				sts.Spec.Template.Spec.Containers[0].SecurityContext.Privileged = &[]bool{true}[0]
				sts.Spec.Template.Spec.Containers[0].SecurityContext.AllowPrivilegeEscalation = &[]bool{true}[0]
			}
		} else {
			sts.Spec.VolumeClaimTemplates = persistentVolumeClaim(h)
		}
		if h.Spec.Persistence.IsExternal() {
			sts.Spec.Template.Spec.Containers = append(sts.Spec.Template.Spec.Containers, backupAgentContainer(h))
		}
		if h.Spec.Persistence.IsRestoreEnabled() {
			err := validation.ValidateRestoreConfiguration(h.Spec.Persistence.Restore)
			if err != nil {
				logger.Error(err, "Invalid RestoreConfiguration")
				return err
			}
			provider, err := h.Spec.Persistence.Restore.GetProvider()
			if err != nil {
				logger.Error(err, "Failed to create init container for restore operation")
				return err
			}
			if provider == n.GCP {
				sts.Spec.Template.Spec.Volumes = append(sts.Spec.Template.Spec.Volumes, v1.Volume{
					Name: n.GCPCredentialVolumeName,
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							SecretName: h.Spec.Persistence.Restore.Secret,
						},
					},
				})
			}

			sts.Spec.Template.Spec.InitContainers = append(sts.Spec.Template.Spec.InitContainers, restoreAgentContainer(h, provider))
		}
	}

	err := controllerutil.SetControllerReference(h, sts, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to set owner reference on Statefulset: %w", err)
	}

	opResult, err := util.CreateOrUpdate(ctx, r.Client, sts, func() error {
		sts.Spec.Replicas = h.Spec.ClusterSize
		sts.ObjectMeta.Annotations = statefulSetAnnotations(h)
		sts.Spec.Template.Annotations, err = podAnnotations(h)
		if err != nil {
			return err
		}
		sts.Spec.Template.Spec.ImagePullSecrets = h.Spec.ImagePullSecrets
		sts.Spec.Template.Spec.Containers[0].Image = h.DockerImage()
		sts.Spec.Template.Spec.Containers[0].Env = env(h)
		sts.Spec.Template.Spec.Containers[0].ImagePullPolicy = h.Spec.ImagePullPolicy

		if h.Spec.Scheduling != nil {
			sts.Spec.Template.Spec.Affinity = h.Spec.Scheduling.Affinity
			sts.Spec.Template.Spec.Tolerations = h.Spec.Scheduling.Tolerations
			sts.Spec.Template.Spec.NodeSelector = h.Spec.Scheduling.NodeSelector
			sts.Spec.Template.Spec.TopologySpreadConstraints = h.Spec.Scheduling.TopologySpreadConstraints
		} else {
			sts.Spec.Template.Spec.Affinity = nil
			sts.Spec.Template.Spec.Tolerations = nil
			sts.Spec.Template.Spec.NodeSelector = nil
			sts.Spec.Template.Spec.TopologySpreadConstraints = nil
		}

		if h.Spec.Resources != nil {
			sts.Spec.Template.Spec.Containers[0].Resources = *h.Spec.Resources
		} else {
			sts.Spec.Template.Spec.Containers[0].Resources = v1.ResourceRequirements{}
		}
		return nil
	})
	if opResult != controllerutil.OperationResultNone {
		logger.Info("Operation result", "Statefulset", h.Name, "result", opResult)
	}
	return err
}
func restoreAgentVolumeMounts(h *hazelcastv1alpha1.Hazelcast, provider string) []v1.VolumeMount {
	volumeMounts := []v1.VolumeMount{{
		Name:      n.PersistenceVolumeName,
		MountPath: h.Spec.Persistence.BaseDir,
	}}
	if provider == n.GCP {
		volumeMounts = append(volumeMounts, v1.VolumeMount{
			Name:      n.GCPCredentialVolumeName,
			MountPath: n.GCPCredentialVolumePath,
		})
	}
	return volumeMounts
}

func restoreAgentCredentials(secret string, provider string) []v1.EnvVar {
	switch provider {
	case n.AWS:
		return []v1.EnvVar{
			{
				Name: n.BucketDataS3EnvAccessKeyID,
				ValueFrom: &v1.EnvVarSource{
					SecretKeyRef: &v1.SecretKeySelector{
						LocalObjectReference: v1.LocalObjectReference{
							Name: secret,
						},
						Key: n.BucketDataS3AccessKeyID,
					},
				},
			},
			{
				Name: n.BucketDataS3EnvSecretAccessKey,
				ValueFrom: &v1.EnvVarSource{
					SecretKeyRef: &v1.SecretKeySelector{
						LocalObjectReference: v1.LocalObjectReference{
							Name: secret,
						},
						Key: n.BucketDataS3SecretAccessKey,
					},
				},
			},
			{
				Name: n.BucketDataS3EnvRegion,
				ValueFrom: &v1.EnvVarSource{
					SecretKeyRef: &v1.SecretKeySelector{
						LocalObjectReference: v1.LocalObjectReference{
							Name: secret,
						},
						Key: n.BucketDataS3Region,
					},
				},
			},
		}
	case n.GCP:
		return []v1.EnvVar{
			{
				Name:  n.BucketDataGCPEnvCredentialFile,
				Value: n.GCPCredentialVolumePath + "/" + n.BucketDataGCPCredentialFile,
			},
		}
	case n.AZURE:
		return []v1.EnvVar{
			{
				Name: n.BucketDataAzureEnvStorageAccount,
				ValueFrom: &v1.EnvVarSource{
					SecretKeyRef: &v1.SecretKeySelector{
						LocalObjectReference: v1.LocalObjectReference{
							Name: secret,
						},
						Key: n.BucketDataAzureStorageAccount,
					},
				},
			},
			{
				Name: n.BucketDataAzureEnvStorageKey,
				ValueFrom: &v1.EnvVarSource{
					SecretKeyRef: &v1.SecretKeySelector{
						LocalObjectReference: v1.LocalObjectReference{
							Name: secret,
						},
						Key: n.BucketDataAzureStorageKey,
					},
				},
			},
		}
	default:
		return nil
	}
}

func backupAgentContainer(h *hazelcastv1alpha1.Hazelcast) v1.Container {
	return v1.Container{
		Name:  n.BackupAgent,
		Image: h.AgentDockerImage(),
		Ports: []v1.ContainerPort{{
			ContainerPort: n.DefaultAgentPort,
			Name:          n.BackupAgent,
			Protocol:      v1.ProtocolTCP,
		}},
		Args: []string{"backup"},
		LivenessProbe: &v1.Probe{
			Handler: v1.Handler{
				HTTPGet: &v1.HTTPGetAction{
					Path:   "/health",
					Port:   intstr.FromInt(8080),
					Scheme: corev1.URISchemeHTTP,
				},
			},
			InitialDelaySeconds: 10,
			TimeoutSeconds:      10,
			PeriodSeconds:       10,
			SuccessThreshold:    1,
			FailureThreshold:    10,
		},
		ReadinessProbe: &v1.Probe{
			Handler: v1.Handler{
				HTTPGet: &v1.HTTPGetAction{
					Path:   "/health",
					Port:   intstr.FromInt(8080),
					Scheme: corev1.URISchemeHTTP,
				},
			},
			InitialDelaySeconds: 10,
			TimeoutSeconds:      10,
			PeriodSeconds:       10,
			SuccessThreshold:    1,
			FailureThreshold:    10,
		},
		VolumeMounts: []v1.VolumeMount{{
			Name:      n.PersistenceVolumeName,
			MountPath: h.Spec.Persistence.BaseDir,
		}},
	}
}

func restoreAgentContainer(h *hazelcastv1alpha1.Hazelcast, provider string) v1.Container {
	return v1.Container{
		Name:  n.RestoreAgent,
		Image: h.AgentDockerImage(),
		Args:  []string{"restore"},
		Env: append(restoreAgentCredentials(h.Spec.Persistence.Restore.Secret, provider),
			v1.EnvVar{
				Name:  "RESTORE_BUCKET",
				Value: h.Spec.Persistence.Restore.BucketURI,
			},
			v1.EnvVar{
				Name:  "RESTORE_DESTINATION",
				Value: h.Spec.Persistence.BaseDir,
			},
			v1.EnvVar{
				Name: "RESTORE_HOSTNAME",
				ValueFrom: &v1.EnvVarSource{
					FieldRef: &v1.ObjectFieldSelector{
						FieldPath: "metadata.name",
					},
				},
			},
		),
		VolumeMounts: restoreAgentVolumeMounts(h, provider),
	}
}

func volumes(h *hazelcastv1alpha1.Hazelcast) []v1.Volume {
	return []v1.Volume{
		{
			Name: n.HazelcastStorageName,
			VolumeSource: v1.VolumeSource{
				ConfigMap: &v1.ConfigMapVolumeSource{
					LocalObjectReference: v1.LocalObjectReference{
						Name: h.Name,
					},
				},
			},
		},
	}
}

func hostPathVolume(h *hazelcastv1alpha1.Hazelcast) v1.Volume {
	return v1.Volume{
		Name: n.PersistenceVolumeName,
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: h.Spec.Persistence.HostPath,
				Type: &[]v1.HostPathType{v1.HostPathDirectoryOrCreate}[0],
			},
		},
	}
}

func persistentVolumeClaim(h *hazelcastv1alpha1.Hazelcast) []v1.PersistentVolumeClaim {
	return []v1.PersistentVolumeClaim{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      n.PersistenceVolumeName,
				Namespace: h.Namespace,
				Labels:    labels(h),
			},
			Spec: v1.PersistentVolumeClaimSpec{
				AccessModes: h.Spec.Persistence.Pvc.AccessModes,
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						corev1.ResourceStorage: *h.Spec.Persistence.Pvc.RequestStorage,
					},
				},
				StorageClassName: h.Spec.Persistence.Pvc.StorageClassName,
			},
		},
	}
}

func volumeMount(h *hazelcastv1alpha1.Hazelcast) []corev1.VolumeMount {
	mounts := []v1.VolumeMount{
		{
			Name:      n.HazelcastStorageName,
			MountPath: n.HazelcastMountPath,
		},
	}
	if h.Spec.Persistence.IsEnabled() {
		mounts = append(mounts, v1.VolumeMount{
			Name:      n.PersistenceVolumeName,
			MountPath: h.Spec.Persistence.BaseDir,
		})
	}
	return mounts
}

// checkHotRestart checks if the persistence feature and AutoForceStart is enabled, pods are failing,
// and the cluster is in the PASSIVE mode and performs the Force Start action.
func (r *HazelcastReconciler) checkHotRestart(ctx context.Context, h *hazelcastv1alpha1.Hazelcast, logger logr.Logger) error {
	if !h.Spec.Persistence.IsEnabled() || !h.Spec.Persistence.AutoForceStart {
		return nil
	}
	logger.Info("Persistence and AutoForceStart are enabled. Checking for the cluster HotRestart.")
	for _, member := range h.Status.Members {
		if !member.Ready && member.Reason == "CrashLoopBackOff" {
			logger.Info("Member is crashing with CrashLoopBackOff.",
				"RestartCounts", member.RestartCount, "Message", member.Message)
			rest := NewRestClient(h)
			state, err := rest.GetState(ctx)
			if err != nil {
				return err
			}
			if state != "passive" {
				logger.Info("Force Start can only be triggered on the cluster in PASSIVE state.",
					"State", state)
				return nil
			}
			err = rest.ForceStart(ctx)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *HazelcastReconciler) ensureClusterActive(ctx context.Context, h *hazelcastv1alpha1.Hazelcast, logger logr.Logger) error {
	// make sure restore is active
	if !h.Spec.Persistence.IsRestoreEnabled() {
		return nil
	}

	// make sure restore was successfull
	if h.Status.Restore == nil {
		return nil
	}

	if h.Status.Restore.State != hazelcastv1alpha1.RestoreSucceeded {
		return nil
	}

	if h.Status.Phase == hazelcastv1alpha1.Pending {
		return nil
	}

	// check if all cluster members are in passive state
	for _, member := range h.Status.Members {
		if ClusterState(member.State) != Passive {
			return nil
		}
	}

	rest := NewRestClient(h)
	state, err := rest.GetState(ctx)
	if err != nil {
		return err
	}
	if state != "passive" {
		return nil
	}
	return rest.ChangeState(ctx, Active)
}

func env(h *hazelcastv1alpha1.Hazelcast) []v1.EnvVar {
	envs := []v1.EnvVar{
		{
			Name:  "JAVA_OPTS",
			Value: fmt.Sprintf("-Dhazelcast.config=%s/hazelcast.yaml", n.HazelcastMountPath),
		},
		{
			Name:  "HZ_PARDOT_ID",
			Value: "operator",
		},
		{
			Name:  "HZ_PHONE_HOME_ENABLED",
			Value: strconv.FormatBool(util.IsPhoneHomeEnabled()),
		},
	}
	if h.Spec.LicenseKeySecret != "" {
		envs = append(envs,
			v1.EnvVar{
				Name: hzLicenseKey,
				ValueFrom: &v1.EnvVarSource{
					SecretKeyRef: &v1.SecretKeySelector{
						LocalObjectReference: v1.LocalObjectReference{
							Name: h.Spec.LicenseKeySecret,
						},
						Key: n.LicenseDataKey,
					},
				},
			})
	}

	return envs
}

func labels(h *hazelcastv1alpha1.Hazelcast) map[string]string {
	return map[string]string{
		n.ApplicationNameLabel:         n.Hazelcast,
		n.ApplicationInstanceNameLabel: h.Name,
		n.ApplicationManagedByLabel:    n.OperatorName,
	}
}

func statefulSetAnnotations(h *hazelcastv1alpha1.Hazelcast) map[string]string {
	ans := map[string]string{}
	if h.Spec.ExposeExternally.IsSmart() {
		ans[n.ServicePerPodCountAnnotation] = strconv.Itoa(int(*h.Spec.ClusterSize))
	}
	return ans
}

func podAnnotations(h *hazelcastv1alpha1.Hazelcast) (map[string]string, error) {
	ans := map[string]string{}
	if h.Spec.ExposeExternally.IsSmart() {
		ans[n.ExposeExternallyAnnotation] = string(h.Spec.ExposeExternally.MemberAccessType())
	}
	cfg := config.HazelcastWrapper{Hazelcast: hazelcastConfigMapStruct(h).HazelcastConfigForcingRestart()}
	cfgYaml, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	ans[n.CurrentHazelcastConfigForcingRestartChecksum] = fmt.Sprint(crc32.ChecksumIEEE(cfgYaml))

	return ans, nil
}

func metadata(h *hazelcastv1alpha1.Hazelcast) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:      h.Name,
		Namespace: h.Namespace,
		Labels:    labels(h),
	}
}

func (r *HazelcastReconciler) updateLastSuccessfulConfiguration(ctx context.Context, h *hazelcastv1alpha1.Hazelcast, logger logr.Logger) error {
	hs, err := json.Marshal(h.Spec)
	if err != nil {
		return err
	}

	opResult, err := util.CreateOrUpdate(ctx, r.Client, h, func() error {
		if h.ObjectMeta.Annotations == nil {
			ans := map[string]string{}
			h.ObjectMeta.Annotations = ans
		}
		h.ObjectMeta.Annotations[n.LastSuccessfulSpecAnnotation] = string(hs)
		return nil
	})
	if opResult != controllerutil.OperationResultNone {
		logger.Info("Operation result", "Hazelcast Annotation", h.Name, "result", opResult)
	}
	return err
}
