package config

type HazelcastWrapper struct {
	Hazelcast Hazelcast `yaml:"hazelcast"`
}

type Hazelcast struct {
	Jet         Jet            `yaml:"jet,omitempty"`
	Network     Network        `yaml:"network,omitempty"`
	ClusterName string         `yaml:"cluster-name,omitempty"`
	Persistence Persistence    `yaml:"persistence,omitempty"`
	Map         map[string]Map `yaml:"map,omitempty"`
}

type Jet struct {
	Enabled *bool `yaml:"enabled,omitempty"`
}

type Network struct {
	Join    Join    `yaml:"join,omitempty"`
	RestAPI RestAPI `yaml:"rest-api,omitempty"`
}

type Join struct {
	Kubernetes Kubernetes `yaml:"kubernetes,omitempty"`
}

type Persistence struct {
	Enabled                   *bool  `yaml:"enabled,omitempty"`
	BaseDir                   string `yaml:"base-dir"`
	BackupDir                 string `yaml:"backup-dir,omitempty"`
	Parallelism               int32  `yaml:"parallelism"`
	ValidationTimeoutSec      int32  `yaml:"validation-timeout-seconds"`
	DataLoadTimeoutSec        int32  `yaml:"data-load-timeout-seconds"`
	ClusterDataRecoveryPolicy string `yaml:"cluster-data-recovery-policy"`
	AutoRemoveStaleData       *bool  `yaml:"auto-remove-stale-data"`
}

type Kubernetes struct {
	Enabled                      *bool  `yaml:"enabled,omitempty"`
	Namespace                    string `yaml:"namespace,omitempty"`
	ServiceName                  string `yaml:"service-name,omitempty"`
	UseNodeNameAsExternalAddress *bool  `yaml:"use-node-name-as-external-address,omitempty"`
	ServicePerPodLabelName       string `yaml:"service-per-pod-label-name,omitempty"`
	ServicePerPodLabelValue      string `yaml:"service-per-pod-label-value,omitempty"`
}

type RestAPI struct {
	Enabled        *bool          `yaml:"enabled,omitempty"`
	EndpointGroups EndpointGroups `yaml:"endpoint-groups,omitempty"`
}

type EndpointGroups struct {
	HealthCheck  EndpointGroup `yaml:"HEALTH_CHECK,omitempty"`
	ClusterWrite EndpointGroup `yaml:"CLUSTER_WRITE,omitempty"`
	Persistence  EndpointGroup `yaml:"PERSISTENCE,omitempty"`
}

type EndpointGroup struct {
	Enabled *bool `yaml:"enabled,omitempty"`
}

type Map struct {
	BackupCount       int32         `yaml:"backup-count"`
	AsyncBackupCount  int32         `yaml:"async-backup-count"`
	TimeToLiveSeconds int32         `yaml:"time-to-live-seconds"`
	MaxIdleSeconds    int32         `yaml:"max-idle-seconds"`
	Eviction          MapEviction   `yaml:"eviction,omitempty"`
	ReadBackupData    bool          `yaml:"read-backup-data"`
	InMemoryFormat    string        `yaml:"in-memory-format"`
	StatisticsEnabled bool          `yaml:"statistics-enabled"`
	Indexes           []MapIndex    `yaml:"indexes,omitempty"`
	HotRestart        MapHotRestart `yaml:"hot-restart,omitempty"`
}

type MapEviction struct {
	Size           int32  `yaml:"size"`
	MaxSizePolicy  string `yaml:"max-size-policy,omitempty"`
	EvictionPolicy string `yaml:"eviction-policy,omitempty"`
}

type MapIndex struct {
	Name               string             `yaml:"name,omitempty"`
	Type               string             `yaml:"type"`
	Attributes         []string           `yaml:"attributes"`
	BitmapIndexOptions BitmapIndexOptions `yaml:"bitmap-index-options,omitempty"`
}

type BitmapIndexOptions struct {
	UniqueKey               string `yaml:"unique-key"`
	UniqueKeyTransformation string `yaml:"unique-key-transformation"`
}

type MapHotRestart struct {
	Enabled bool `yaml:"enabled"`
	Fsync   bool `yaml:"fsync"`
}

func (hz Hazelcast) HazelcastConfigForcingRestart() Hazelcast {
	return Hazelcast{
		ClusterName: hz.ClusterName,
		Network: Network{
			Join: Join{
				Kubernetes: Kubernetes{
					ServicePerPodLabelName:       hz.Network.Join.Kubernetes.ServicePerPodLabelName,
					ServicePerPodLabelValue:      hz.Network.Join.Kubernetes.ServicePerPodLabelValue,
					UseNodeNameAsExternalAddress: hz.Network.Join.Kubernetes.UseNodeNameAsExternalAddress,
				},
			},
		},
	}
}
