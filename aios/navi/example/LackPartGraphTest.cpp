#include <fstream>
#include <iostream>
#include <unistd.h>

#include "autil/StringUtil.h"
#include "autil/ThreadPool.h"
#include "autil/EnvUtil.h"
#include "autil/Scope.h"
#include "navi/builder/GraphBuilder.h"
#include "navi/engine/Navi.h"
#include "navi/engine/NaviSession.h"
#include "navi/example/TestData.h"
#include "navi/example/TestResource.h"
#include "navi/proto/GraphDef.pb.h"
#include "navi/test_cluster/NaviTestCluster.h"
#include "navi/util/CommonUtil.h"
#include "navi/util/NaviClosure.h"
#include "navi/test_cluster/NaviFuncClosure.h"
#include "unittest/unittest.h"

using namespace std;
using namespace multi_call;

namespace navi {

class ModuleManager;

class LackPartGraphTest : public TESTBASE
{
public:
    void setUp();
    void tearDown();
private:
    void initCluster();
    void buildCluster(NaviTestCluster &cluster);
    void sortRpcInfoMap(GigStreamRpcInfoMap &rpcInfoMap);

protected:
    std::string _loader;
    std::unique_ptr<GraphDef> _graphDef;
private:
    std::unique_ptr<autil::EnvGuard> _naviPythonHome;
};


void LackPartGraphTest::setUp()
{
    _naviPythonHome.reset(new autil::EnvGuard(
                    "NAVI_PYTHON_HOME",
                    TEST_ROOT_PATH() + "config_loader/python:/usr/lib64/python3.6/"));
    _loader = GET_PRIVATE_TEST_DATA_PATH() + "test_config_loader.py";
    _graphDef.reset(new GraphDef());
}

void LackPartGraphTest::tearDown() {
}

void LackPartGraphTest::buildCluster(NaviTestCluster &cluster)
{
    ResourceMapPtr rootResourceMap(new ResourceMap());
    std::string configPath = GET_PRIVATE_TEST_DATA_PATH() + "config/cluster/";
    std::string biz1Config = GET_PRIVATE_TEST_DATA_PATH() + "config/cluster/biz1.py";

    ASSERT_TRUE(cluster.addServer("host_0", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(cluster.addServer("host_1", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(cluster.addServer("host_2", _loader, configPath, rootResourceMap));

    ASSERT_TRUE(cluster.addBiz("host_0", "biz_qrs", 1, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_1", "biz_a", 3, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_2", "biz_a", 3, 1, biz1Config));

    ASSERT_TRUE(cluster.start());

    rootResourceMap.reset();
}

void LackPartGraphTest::sortRpcInfoMap(GigStreamRpcInfoMap &rpcInfoMap) {
    for (auto &pair : rpcInfoMap) {
        sort(pair.second.begin(), pair.second.end(),
             [](const auto &lhs, const auto &rhs) {
                 return lhs.partId < rhs.partId;
             });
    }
}

// biz_a(sourceKernel) -> biz_qrs(identityTestKernel)
TEST_F(LackPartGraphTest, testBizA2BizQ)
{
    NaviTestCluster cluster;
    ASSERT_NO_FATAL_FAILURE(buildCluster(cluster));

    GraphBuilder builder(_graphDef.get());

    builder.newSubGraph("biz_qrs");
    auto identity = builder.node("identity_qrs")
                    .kernel("IdentityTestKernel");
    identity.out("output1").asGraphOutput("GraphOutput1");

    builder.newSubGraph("biz_a");
    auto source = builder.node("source_a").kernel("SourceKernel").jsonAttrs(R"json({"times" : 2})json");
    source.out("output1").to(identity.in("input1"));

    ASSERT_TRUE(builder.ok());

    RunGraphParams params;
    params.setTimeoutMs(10000000);
    // params.setTraceLevel("SCHEDULE2");

    auto navi = cluster.getNavi("host_0");

    auto userResult = navi->runGraph(_graphDef.release(), params);

    std::vector<NaviUserData> dataVec;
    GraphDef graphDef;
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

    // times * biz_a * qrs
    // 2 * (3 - 1) * 1 = 4
    ASSERT_EQ(4, dataVec.size());

    auto naviResult = userResult->getNaviResult();

    ASSERT_EQ("EC_NONE", std::string(CommonUtil::getErrorString(naviResult->ec)));
    ASSERT_EQ("", naviResult->errorEvent.message);

    auto &rpcInfoMap = naviResult->rpcInfoMap;
    ASSERT_NO_FATAL_FAILURE(sortRpcInfoMap(rpcInfoMap));
    {
        auto it = rpcInfoMap.find(make_pair("biz_qrs(-1)", "biz_a"));
        ASSERT_TRUE(it != rpcInfoMap.end());
        auto &rpcVec = it->second;
        ASSERT_EQ(3, rpcVec.size());
        ASSERT_EQ(0, rpcVec[0].partId);
        ASSERT_FALSE(rpcVec[0].isLack());
        ASSERT_EQ(1, rpcVec[1].partId);
        ASSERT_FALSE(rpcVec[1].isLack());
        ASSERT_EQ(2, rpcVec[2].partId);
        ASSERT_TRUE(rpcVec[2].isLack());
    }

    naviResult->show();
}

} // namespace navi
