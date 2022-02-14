package ph

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	hazelcastcomv1alpha1 "github.com/hazelcast/hazelcast-platform-operator/api/v1alpha1"
	n "github.com/hazelcast/hazelcast-platform-operator/controllers/naming"
	hazelcastconfig "github.com/hazelcast/hazelcast-platform-operator/test/e2e/config/hazelcast"
	mcconfig "github.com/hazelcast/hazelcast-platform-operator/test/e2e/config/managementcenter"
)

var _ = Describe("Hazelcast", func() {

	var lookupKeyHz = types.NamespacedName{
		Name:      hzName,
		Namespace: hzNamespace,
	}
	var lookupKeyMc = types.NamespacedName{
		Name:      mcName,
		Namespace: hzNamespace,
	}

	var controllerManagerName = types.NamespacedName{
		Name:      controllerManagerName(),
		Namespace: hzNamespace,
	}

	BeforeEach(func() {
		if !useExistingCluster() {
			Skip("End to end tests require k8s cluster. Set USE_EXISTING_CLUSTER=true")
		}
		if runningLocally() {
			return
		}
		By("Checking hazelcast-platform-controller-manager running", func() {
			controllerDep := &appsv1.Deployment{}
			Eventually(func() (int32, error) {
				return getDeploymentReadyReplicas(context.Background(), controllerManagerName, controllerDep)
			}, timeout, interval).Should(Equal(int32(1)))
		})
	})

	createHz := func(hazelcast *hazelcastcomv1alpha1.Hazelcast) {
		By("Creating Hazelcast", func() {
			Expect(k8sClient.Create(context.Background(), hazelcast)).Should(Succeed())
		})

		By("Checking Hazelcast is running", func() {
			hz := &hazelcastcomv1alpha1.Hazelcast{}
			Eventually(func() bool {
				err := k8sClient.Get(context.Background(), lookupKeyHz, hz)
				Expect(err).ToNot(HaveOccurred())
				return isHazelcastRunning(hz)
			}, timeout, interval).Should(BeTrue())
		})
	}

	evaluateReadyMembers := func() {
		hz := &hazelcastcomv1alpha1.Hazelcast{}
		Eventually(func() string {
			err := k8sClient.Get(context.Background(), lookupKeyHz, hz)
			Expect(err).ToNot(HaveOccurred())
			return hz.Status.Cluster.ReadyMembers
		}, timeout, interval).Should(Equal("3/3"))
	}

	assertAnnotationExists := func() {
		hz := &hazelcastcomv1alpha1.Hazelcast{}
		Eventually(func() bool {
			err := k8sClient.Get(context.Background(), lookupKeyHz, hz)
			if err != nil {
				return false
			}
			test := hz.ObjectMeta.Annotations
			fmt.Println(test)
			_, ok := hz.ObjectMeta.Annotations[n.LastSuccessfulSpecAnnotation]
			return ok
		}, timeout, interval).Should(BeTrue())
	}

	createMc := func(mancenter *hazelcastcomv1alpha1.ManagementCenter) {
		By("Creating ManagementCenter", func() {
			Expect(k8sClient.Create(context.Background(), mancenter)).Should(Succeed())
		})

		By("Checking ManagementCenter is running", func() {
			mc := &hazelcastcomv1alpha1.ManagementCenter{}
			Eventually(func() bool {
				err := k8sClient.Get(context.Background(), lookupKeyMc, mc)
				Expect(err).ToNot(HaveOccurred())
				return isManagementCenterRunning(mc)
			}, timeout, interval).Should(BeTrue())
		})
	}

	Describe("Phone Home Table with installed Hazelcast", func() {
		AfterEach(func() {
			Expect(k8sClient.Delete(context.Background(), emptyHazelcast(), client.PropagationPolicy(v1.DeletePropagationForeground))).Should(Succeed())
			assertDoesNotExist(lookupKeyHz, &hazelcastcomv1alpha1.Hazelcast{})
		})
		DescribeTable("should have correct metrics",

			func(h *hazelcastcomv1alpha1.Hazelcast,
				createdEnterpriseClusterCount int,
				unisocket int,
				smart int,
				discoveryLoadBalancer int,
				discoveryNodePort int,
				memberNodePortExternalIP int,
				memberNodePortNodeName int,
				memberLoadBalancer int) {

				createHz(h)
				hzCreationTime := time.Now().UTC().Truncate(time.Hour)
				evaluateReadyMembers()
				assertAnnotationExists()

				bigQueryTable := getBigQueryTable()
				Expect(bigQueryTable.IP).Should(MatchRegexp("^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$"), "IP address should be present and match regexp")
				Expect(bigQueryTable.PingTime.Truncate(time.Hour)).Should(BeTemporally("~", hzCreationTime), "Ping time should be near to current date")
				Expect(bigQueryTable.OperatorID).Should(Equal(getOperatorId()), "Operator UID metric")
				Expect(bigQueryTable.PardotID).Should(Equal("dockerhub"), "Pardot ID metric")
				Expect(bigQueryTable.Version).Should(Equal("latest-snapshot"), "Version metric")
				Expect(bigQueryTable.Uptime).ShouldNot(BeZero(), "Version metric")
				Expect(bigQueryTable.K8sDistribution).Should(Equal("GKE"), "K8sDistribution metric")
				Expect(bigQueryTable.K8sVersion).ShouldNot(BeEmpty(), "K8sVersion metric")
				Expect(bigQueryTable.CreatedClusterCount).Should(Equal(0), "CreatedClusterCount metric")
				Expect(bigQueryTable.CreatedEnterpriseClusterCount).Should(Equal(createdEnterpriseClusterCount), "CreatedEnterpriseClusterCount metric")
				Expect(bigQueryTable.AverageClusterCreationLatency).ShouldNot(BeZero(), "AverageClusterCreationLatency metric")
				Expect(bigQueryTable.AverageMCCreationLatency).Should(Equal(bigquery.NullInt64{}), "AverageMCCreationLatency metric")
				Expect(bigQueryTable.CreatedMemberCount).Should(Equal(3), "CreatedMemberCount metric")
				Expect(bigQueryTable.CreatedMCCount).Should(Equal(0), "CreatedMCCount metric")
				Expect(bigQueryTable.ExposeExternally.Unisocket).Should(Equal(unisocket), "Unisocket metric")
				Expect(bigQueryTable.ExposeExternally.Smart).Should(Equal(smart), "Smart metric")
				Expect(bigQueryTable.ExposeExternally.DiscoveryLoadBalancer).Should(Equal(discoveryLoadBalancer), "DiscoveryLoadBalancer metric")
				Expect(bigQueryTable.ExposeExternally.DiscoveryNodePort).Should(Equal(discoveryNodePort), "DiscoveryNodePort metric")
				Expect(bigQueryTable.ExposeExternally.MemberNodePortExternalIP).Should(Equal(memberNodePortExternalIP), "MemberNodePortExternalIP metric")
				Expect(bigQueryTable.ExposeExternally.MemberNodePortNodeName).Should(Equal(memberNodePortNodeName), "MemberNodePortNodeName metric")
				Expect(bigQueryTable.ExposeExternally.MemberLoadBalancer).Should(Equal(memberLoadBalancer), "MemberLoadBalancer metric")
			},
			Entry("with ExposeExternallyUnisocket configuration", hazelcastconfig.ExposeExternallyUnisocket(hzNamespace, ee), 1, 1, 0, 1, 0, 0, 0, 0),
			Entry("with ExposeExternallySmartNodePort configuration", hazelcastconfig.ExposeExternallySmartNodePort(hzNamespace, ee), 1, 0, 1, 1, 0, 1, 0, 0),
			Entry("with ExposeExternallySmartLoadBalancer configuration", hazelcastconfig.ExposeExternallySmartLoadBalancer(hzNamespace, ee), 1, 0, 1, 1, 0, 0, 0, 1),
			Entry("with ExposeExternallySmartNodePortNodeName configuration", hazelcastconfig.ExposeExternallySmartNodePortNodeName(hzNamespace, ee), 1, 0, 1, 0, 1, 0, 1, 0),
		)
	})

	Describe("Phone Home table with installed Management Center", func() {
		AfterEach(func() {
			Expect(k8sClient.Delete(context.Background(), emptyManagementCenter(), client.PropagationPolicy(v1.DeletePropagationForeground))).Should(Succeed())
			assertDoesNotExist(lookupKeyMc, &hazelcastcomv1alpha1.ManagementCenter{})
			pvcLookupKey := types.NamespacedName{
				Name:      fmt.Sprintf("mancenter-storage-%s-0", lookupKeyMc.Name),
				Namespace: lookupKeyMc.Namespace,
			}
			deleteIfExists(pvcLookupKey, &corev1.PersistentVolumeClaim{})
		})
		It("should have correct metrics", func() {
			mc := mcconfig.Default(hzNamespace, ee)
			createMc(mc)
			mcCreationTime := time.Now().Truncate(time.Hour)
			bigQueryTable := getBigQueryTable()
			Expect(bigQueryTable.IP).Should(MatchRegexp("^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$"), "IP address should be present and match regexp")
			Expect(bigQueryTable.PingTime.Truncate(time.Hour)).Should(BeTemporally("~", mcCreationTime), "Ping time should be near to current date")
			Expect(bigQueryTable.OperatorID).Should(Equal(getOperatorId()), "Operator UID should be equal to Hazelcast Operator UID")
			Expect(bigQueryTable.PardotID).Should(Equal("dockerhub"), "Pardot ID metric")
			Expect(bigQueryTable.Version).Should(Equal("latest-snapshot"), "Version metric")
			Expect(bigQueryTable.Uptime).ShouldNot(BeZero(), "Uptime metric")
			Expect(bigQueryTable.K8sDistribution).Should(Equal("GKE"), "K8sDistribution metric")
			Expect(bigQueryTable.K8sVersion).ShouldNot(BeEmpty(), "K8sVersion metric")
			Expect(bigQueryTable.CreatedClusterCount).Should(Equal(0), "CreatedClusterCount metric")
			Expect(bigQueryTable.CreatedEnterpriseClusterCount).Should(Equal(0), "CreatedEnterpriseClusterCount metric")
			Expect(bigQueryTable.AverageClusterCreationLatency).Should(Equal(bigquery.NullInt64{}), "AverageClusterCreationLatency metric")
			Expect(bigQueryTable.AverageMCCreationLatency).ShouldNot(BeZero(), "AverageMCCreationLatency metric")
			Expect(bigQueryTable.CreatedMemberCount).Should(Equal(0), "CreatedMemberCount metric")
			Expect(bigQueryTable.CreatedMCCount).Should(Equal(1), "CreatedMCCount metric")
			Expect(bigQueryTable.ExposeExternally.Unisocket).Should(Equal(0), "Unisocket metric")
			Expect(bigQueryTable.ExposeExternally.Smart).Should(Equal(0), "Smart metric")
			Expect(bigQueryTable.ExposeExternally.DiscoveryLoadBalancer).Should(Equal(0), "DiscoveryLoadBalancer metric")
			Expect(bigQueryTable.ExposeExternally.DiscoveryNodePort).Should(Equal(0), "DiscoveryNodePort metric")
			Expect(bigQueryTable.ExposeExternally.MemberNodePortExternalIP).Should(Equal(0), "MemberNodePortExternalIP metric")
			Expect(bigQueryTable.ExposeExternally.MemberNodePortNodeName).Should(Equal(0), "MemberNodePortNodeName metric")
			Expect(bigQueryTable.ExposeExternally.MemberLoadBalancer).Should(Equal(0), "MemberLoadBalancer metric")
		})
	})
})