#include <fstream>
#include <iostream>
#include <unistd.h>

#include "autil/StringUtil.h"
#include "autil/ThreadPool.h"
#include "autil/EnvUtil.h"
#include "navi/builder/GraphBuilder.h"
#include "navi/engine/Navi.h"
#include "navi/example/TestData.h"
#include "navi/example/TestResource.h"
#include "navi/proto/GraphDef.pb.h"
#include "navi/test_cluster/NaviTestCluster.h"
#include "navi/util/CommonUtil.h"
#include "unittest/unittest.h"

using namespace std;

namespace navi {

class ModuleManager;

class DistributeGraphTest : public TESTBASE
{
public:
    void setUp();
    void tearDown();
private:
    void initCluster();
    void buildCluster(NaviTestCluster &cluster);
protected:
    std::string _loader;
    std::unique_ptr<GraphDef> _graphDef;
private:
    std::unique_ptr<autil::EnvGuard> _naviPythonHome;
};


void DistributeGraphTest::setUp()
{
    _naviPythonHome.reset(new autil::EnvGuard(
                    "NAVI_PYTHON_HOME",
                    TEST_ROOT_PATH() + "config_loader/python:/usr/lib64/python3.6/"));
    _loader = GET_PRIVATE_TEST_DATA_PATH() + "test_config_loader.py";
    _graphDef.reset(new GraphDef());
}

void DistributeGraphTest::tearDown() {
}

void DistributeGraphTest::buildCluster(NaviTestCluster &cluster)
{
    ResourceMapPtr rootResourceMap(new ResourceMap());
    std::string configPath = GET_PRIVATE_TEST_DATA_PATH() + "config/cluster/";
    std::string biz1Config = GET_PRIVATE_TEST_DATA_PATH() + "config/cluster/biz1.py";

    ASSERT_TRUE(cluster.addServer("host_0", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(cluster.addServer("host_1", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(cluster.addServer("host_2", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(cluster.addServer("host_3", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(cluster.addServer("host_4", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(cluster.addServer("host_5", _loader, configPath, rootResourceMap));

    ASSERT_TRUE(cluster.addBiz("host_0", "biz_qrs", 1, 0, biz1Config));

    ASSERT_TRUE(cluster.addBiz("host_1", "biz_a", 2, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_2", "biz_a", 2, 1, biz1Config));

    ASSERT_TRUE(cluster.addBiz("host_5", "biz_b", 3, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_5", "biz_b", 3, 1, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_5", "biz_b", 3, 2, biz1Config));

    ASSERT_TRUE(cluster.start());

    rootResourceMap.reset();
}

TEST_F(DistributeGraphTest, testSimple)
{
    NaviTestCluster cluster;
    ASSERT_NO_FATAL_FAILURE(buildCluster(cluster));

    GraphBuilder builder(_graphDef.get());
    builder.newSubGraph("biz_qrs");
    auto qrsMerge = builder.node("qrs_merge").kernel("MergeKernel");
    qrsMerge.out("output1").asGraphOutput("GraphOutput1");

    builder.newSubGraph("biz_b");
    builder.partIds({0, 1});
    auto bIdentity = builder.node("b_identity").kernel("IdentityTestKernel");
    bIdentity.out("output1").to(qrsMerge.in("input2"));

    builder.newSubGraph("biz_a");
    builder.partIds({0});
    auto aSource = builder.node("a_source").kernel("SourceKernel").jsonAttrs(R"json({"times" : 2})json");
    aSource.out("output1").to(bIdentity.in("input1"));

    builder.newSubGraph("biz_a");
    builder.partIds({0});
    auto a2Source = builder.node("a2_source").kernel("SourceKernel").jsonAttrs(R"json({"times" : 2})json");
    auto a2Identity = builder.node("a2_identity").kernel("IdentityTestKernel");
    a2Source.out("output1").to(a2Identity.in("input1"));
    a2Identity.out("output1").to(qrsMerge.in("input1"));

    ASSERT_TRUE(builder.ok());

    RunGraphParams params;
    params.setTimeoutMs(100000);
    // params.setTraceLevel("SCHEDULE2");

    auto navi = cluster.getNavi("host_0");

    auto userResult = navi->runGraph(_graphDef.release(), params);

    std::vector<NaviUserData> dataVec;
    while (true) {
        NaviUserData data;
        bool eof = false;
        if (userResult->nextData(data, eof) && data.data) {
            dataVec.push_back(data);
        }
        if (eof) {
            break;
        }
    }

    EXPECT_EQ(1, dataVec.size());

    auto naviResult = userResult->getNaviResult();

    EXPECT_EQ("EC_NONE", std::string(CommonUtil::getErrorString(naviResult->ec)));
    EXPECT_EQ("", naviResult->errorEvent.message);

    naviResult->show();
}

// biz_a(sourceKernel) -> biz_qrs(identityTestKernel)
TEST_F(DistributeGraphTest, testBizA2BizQ)
{
    NaviTestCluster cluster;
    ASSERT_NO_FATAL_FAILURE(buildCluster(cluster));

    GraphBuilder builder(_graphDef.get());

    builder.newSubGraph("biz_qrs");
    auto identityQrs = builder.node("identity_qrs").kernel("IdentityTestKernel");
    identityQrs.out("output1").asGraphOutput("GraphOutput1");

    builder.newSubGraph("biz_a");
    auto sourceA = builder.node("source_a").kernel("SourceKernel").jsonAttrs(R"json({"times" : 1})json");
    sourceA.out("output1").to(identityQrs.in("input1"));

    ASSERT_TRUE(builder.ok());

    RunGraphParams params;
    params.setTimeoutMs(100000);
    // params.setTraceLevel("SCHEDULE2");

    auto navi = cluster.getNavi("host_0");

    auto userResult = navi->runGraph(_graphDef.release(), params);

    std::vector<NaviUserData> dataVec;
    while (true) {
        NaviUserData data;
        bool eof = false;
        if (userResult->nextData(data, eof) && data.data) {
            dataVec.push_back(data);
        }
        if (eof) {
            break;
        }
    }
    // biz_a * qrs
    // 2 * 1 = 2
    ASSERT_EQ(2, dataVec.size());

    auto naviResult = userResult->getNaviResult();

    EXPECT_EQ("EC_NONE", std::string(CommonUtil::getErrorString(naviResult->ec)));
    EXPECT_EQ("", naviResult->errorEvent.message);

    naviResult->show();
}

TEST_F(DistributeGraphTest, testNamedData)
{
    NaviTestCluster cluster;
    ASSERT_NO_FATAL_FAILURE(buildCluster(cluster));

    GraphBuilder builder(_graphDef.get());
    auto mainGraph = builder.newSubGraph("biz_a");
    auto outputPort = builder.subGraph(mainGraph)
                          .node("source")
                          .kernel("SourceKernel")
                          .jsonAttrs(R"json({"times" : 20})json")
                          .out("output1");
    auto namedNode = builder.node("named_data").kernel("NamedDataKernelSub");
    namedNode.in("input1").from(outputPort);
    namedNode.out("output1").asGraphOutput("o1");
    ASSERT_TRUE(builder.ok());

    RunGraphParams params;
    params.setTimeoutMs(100000);
    // params.setTraceLevel("SCHEDULE2");

    NamedData namedData;
    namedData.graphId = 0;
    namedData.name = "test_named_data";
    namedData.data.reset(new HelloData("I'm named data: test_named_data"));
    params.addNamedData(namedData);

    auto navi = cluster.getNavi("host_0");

    auto userResult = navi->runGraph(_graphDef.release(), params);

    std::vector<NaviUserData> dataVec;
    while (true) {
        NaviUserData data;
        bool eof = false;
        if (userResult->nextData(data, eof) && data.data) {
            data.data->show();
            dataVec.push_back(data);
        }
        if (eof) {
            break;
        }
    }
    // biz_a * qrs
    // 2 * 1 = 2
    auto naviResult = userResult->getNaviResult();
    EXPECT_EQ("EC_NONE", std::string(CommonUtil::getErrorString(naviResult->ec)));
    EXPECT_EQ("", naviResult->errorEvent.message);
    EXPECT_EQ(40, dataVec.size());
    naviResult->show();
}

} // namespace navi
