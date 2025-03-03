package e2e

import (
	"context"
	"fmt"
	. "time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	hazelcastcomv1alpha1 "github.com/hazelcast/hazelcast-platform-operator/api/v1alpha1"
	hazelcastconfig "github.com/hazelcast/hazelcast-platform-operator/test/e2e/config/hazelcast"
)

var _ = Describe("Hazelcast CR with expose externally feature", Label("hz_expose_externally"), func() {
	hzName := fmt.Sprintf("hz-ex-ex-%d", GinkgoParallelProcess())

	var hzLookupKey = types.NamespacedName{
		Name:      hzName,
		Namespace: hzNamespace,
	}
	labels := map[string]string{
		"test_suite": fmt.Sprintf("hz_expose_externally_%d", GinkgoParallelProcess()),
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
			}, 90*Second, interval).Should(Equal(int32(1)))
		})
	})

	AfterEach(func() {
		Expect(k8sClient.Delete(context.Background(), emptyHazelcast(hzLookupKey), client.PropagationPolicy(v1.DeletePropagationForeground))).Should(Succeed())
		assertDoesNotExist(hzLookupKey, &hazelcastcomv1alpha1.Hazelcast{})
	})
	ctx := context.Background()
	assertExternalAddressesNotEmpty := func() {
		By("status external addresses should not be empty")
		Eventually(func() string {
			hz := &hazelcastcomv1alpha1.Hazelcast{}
			err := k8sClient.Get(context.Background(), hzLookupKey, hz)
			Expect(err).ToNot(HaveOccurred())
			return hz.Status.ExternalAddresses
		}, 2*Minute, interval).Should(Not(BeEmpty()))
	}

	It("should create Hazelcast cluster and allow connecting with Hazelcast unisocket client", Label("slow"), func() {
		assertUseHazelcastUnisocket := func() {
			FillTheMapData(ctx, hzLookupKey, false, "map", 100)
		}
		hazelcast := hazelcastconfig.ExposeExternallyUnisocket(hzLookupKey, ee, labels)
		CreateHazelcastCR(hazelcast)
		assertUseHazelcastUnisocket()
		assertExternalAddressesNotEmpty()
	})

	It("should create Hazelcast cluster exposed with NodePort services and allow connecting with Hazelcast smart client", Label("slow"), func() {
		assertUseHazelcastSmart := func() {
			FillTheMapData(ctx, hzLookupKey, false, "map", 100)
		}
		hazelcast := hazelcastconfig.ExposeExternallySmartNodePort(hzLookupKey, ee, labels)
		CreateHazelcastCR(hazelcast)
		assertUseHazelcastSmart()
		assertExternalAddressesNotEmpty()
	})

	It("should create Hazelcast cluster exposed with LoadBalancer services and allow connecting with Hazelcast smart client", Label("slow"), func() {
		assertUseHazelcastSmart := func() {
			FillTheMapData(ctx, hzLookupKey, false, "map", 100)
		}
		hazelcast := hazelcastconfig.ExposeExternallySmartLoadBalancer(hzLookupKey, ee, labels)
		CreateHazelcastCR(hazelcast)
		assertUseHazelcastSmart()
	})
})
