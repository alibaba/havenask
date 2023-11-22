#include "unittest/unittest.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "navi/example/TestData.h"

using namespace std;
using namespace testing;

namespace navi {

class NaviSnapshotTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void NaviSnapshotTest::setUp() {
}

void NaviSnapshotTest::tearDown() {
}

TEST_F(NaviSnapshotTest, testInitResource) {
    std::string gigClientConfig(R"json({"init_config" : {}})json");
    {
        KernelTesterBuilder testerBuilder;
        testerBuilder.logLevel("debug");
        testerBuilder.threadNum(10);
        testerBuilder.module(
            NAVI_TEST_DATA_PATH + "config/modules/libtest_plugin.so");
        testerBuilder.snapshotResourceConfig(GIG_CLIENT_RESOURCE_ID, gigClientConfig);
        ResourcePtr a1;
        {
            std::cout << "create a1" << std::endl;
            auto resourceMap = testerBuilder.buildResource({ "A" });
            ASSERT_TRUE(resourceMap);
            auto resource = resourceMap->getResource("A");
            ASSERT_TRUE(resource);
            a1 = resource;
        }
        ResourcePtr a2;
        {
            std::cout << "create a2" << std::endl;
            auto resourceMap = testerBuilder.buildResource({ "A" });
            ASSERT_TRUE(resourceMap);
            auto resource = resourceMap->getResource("A");
            ASSERT_TRUE(resource);
            a2 = resource;
        }
        ASSERT_TRUE(a1 != a2);
    }
    {
        KernelTesterBuilder testerBuilder;
        testerBuilder.logLevel("debug");
        testerBuilder.module(
            NAVI_TEST_DATA_PATH + "config/modules/libtest_plugin.so");
        testerBuilder.snapshotResourceConfig(GIG_CLIENT_RESOURCE_ID, gigClientConfig);
        ResourcePtr a1;
        {
            std::cout << "create a1" << std::endl;
            auto resourceMap = testerBuilder.buildResource({ "A" });
            ASSERT_TRUE(resourceMap);
            auto resource = resourceMap->getResource("A");
            ASSERT_TRUE(resource);
            a1 = resource;
        }
        ResourcePtr a2;
        {
            std::cout << "create a2" << std::endl;
            auto resourceMap = testerBuilder.buildResource({ "A" });
            ASSERT_TRUE(resourceMap);
            auto resource = resourceMap->getResource("A");
            ASSERT_TRUE(resource);
            a2 = resource;
        }
        // ASSERT_TRUE(a1 == a2);
    }
}

}
