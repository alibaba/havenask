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

class PartAccessGraphTest : public TESTBASE
{
public:
    void setUp();
    void tearDown();
private:
    void initCluster();
    void buildCluster(NaviTestCluster &cluster);
    void checkGraphDef(GraphDef* graphDef);
    void buildSimpleGraph(GraphDef *graphDef);
    void sortRpcInfoMap(GigStreamRpcInfoMap &rpcInfoMap);

protected:
    std::string _loader;
    std::unique_ptr<GraphDef> _graphDef;
private:
    std::unique_ptr<autil::EnvGuard> _naviPythonHome;
};


void PartAccessGraphTest::setUp()
{
    _naviPythonHome.reset(new autil::EnvGuard(
                    "NAVI_PYTHON_HOME",
                    TEST_ROOT_PATH() + "config_loader/python:/usr/lib64/python3.6/"));
    _loader = GET_PRIVATE_TEST_DATA_PATH() + "test_config_loader.py";
    _graphDef.reset(new GraphDef());
}

void PartAccessGraphTest::tearDown() {
}

void PartAccessGraphTest::buildCluster(NaviTestCluster &cluster)
{
    ResourceMapPtr rootResourceMap(new ResourceMap());
    rootResourceMap->addResource(std::make_shared<External>());
    std::string configPath = GET_PRIVATE_TEST_DATA_PATH() + "config/cluster/";
    std::string biz1Config = GET_PRIVATE_TEST_DATA_PATH() + "config/cluster/biz1.py";

    ASSERT_TRUE(cluster.addServer("host_0", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(cluster.addServer("host_1", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(cluster.addServer("host_2", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(cluster.addServer("host_3", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(cluster.addServer("host_4", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(cluster.addServer("host_5", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(cluster.addServer("host_6", _loader, configPath, rootResourceMap));
    ASSERT_TRUE(cluster.addServer("host_7", _loader, configPath, rootResourceMap));

    ASSERT_TRUE(cluster.addBiz("host_0", "biz_qrs", 3, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_1", "biz_qrs", 3, 1, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_2", "biz_qrs", 3, 2, biz1Config));

    ASSERT_TRUE(cluster.addBiz("host_3", "biz_a", 2, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_4", "biz_a", 2, 1, biz1Config));

    ASSERT_TRUE(cluster.addBiz("host_5", "biz_b", 3, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_6", "biz_b", 3, 1, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_7", "biz_b", 3, 2, biz1Config));

    ASSERT_TRUE(cluster.start());

    rootResourceMap.reset();
}

void PartAccessGraphTest::sortRpcInfoMap(GigStreamRpcInfoMap &rpcInfoMap) {
    for (auto &pair : rpcInfoMap) {
        sort(pair.second.begin(), pair.second.end(),
             [](const auto &lhs, const auto &rhs) {
                 return lhs.partId < rhs.partId;
             });
    }
}

// biz_a(sourceKernel) -> biz_qrs(identityTestKernel)
TEST_F(PartAccessGraphTest, testBizA2BizQ)
{
    NaviTestCluster cluster;
    ASSERT_NO_FATAL_FAILURE(buildCluster(cluster));

    usleep(3000 * 1000);
    GraphBuilder builder(_graphDef.get());

    builder.newSubGraph("biz_qrs");
    auto identityQrs = builder.node("identity_qrs").kernel("IdentityTestKernel");
    identityQrs.out("output1").asGraphOutput("GraphOutput1");

    auto aBizGraph = builder.newSubGraph("biz_a");
    builder.subGraph(aBizGraph).partIds({1});
    builder.node("source_a")
        .kernel("SourceKernel")
        .jsonAttrs(R"json({"times" : 2})json")
        .out("output1")
        .to(identityQrs.in("input1"));

    ASSERT_TRUE(builder.ok());

    RunGraphParams params;
    params.setTimeoutMs(10000000);
    // params.setTraceLevel("SCHEDULE2");

    auto navi = cluster.getNavi("host_0");

    auto userResult = navi->runGraph(_graphDef.release(), params);

    std::vector<NaviUserData> dataVec;
    bool graphDefInit = false;
    GraphDef graphDef;
    while (true) {
        NaviUserData data;
        bool eof = false;
        if (userResult->nextData(data, eof) && data.data) {
            if (!graphDefInit) {
                auto naviSession = dynamic_cast<NaviSession *>(userResult->_gdbPtr);
                graphDef = *naviSession->_graphDef;
                graphDefInit = true;
            }
            dataVec.push_back(data);
        }
        if (eof) {
            break;
        }
    }

    ASSERT_TRUE(graphDefInit);
    ASSERT_NO_FATAL_FAILURE(checkGraphDef(&graphDef));

    // times * biz_a * qrs
    // 2 * 1 * 3 = 6
    ASSERT_EQ(6, dataVec.size());

    auto naviResult = userResult->getNaviResult();

    EXPECT_EQ("EC_NONE", std::string(CommonUtil::getErrorString(naviResult->ec)));
    EXPECT_EQ("", naviResult->errorEvent.message);

    naviResult->show();
}

void PartAccessGraphTest::checkGraphDef(GraphDef* graphDef) {
    ASSERT_EQ(3, graphDef->sub_graphs_size());
    {
        const auto &qrsGraph = graphDef->sub_graphs(0);
        ASSERT_EQ("biz_qrs", qrsGraph.location().biz_name());
        ASSERT_EQ(2, qrsGraph.borders_size());
        {
            // biz_qrs --> user_biz
            const auto &border = qrsGraph.borders(0);
            ASSERT_EQ(-2, border.peer().graph_id());
            const auto &partInfo = border.peer().part_info();
            ASSERT_EQ(1, partInfo.part_count());
            ASSERT_EQ(1, partInfo.part_ids_size());
        }
        {
            // biz_a --> biz_qrs
            const auto &border = qrsGraph.borders(1);
            ASSERT_EQ(1, border.peer().graph_id());
            const auto &partInfo = border.peer().part_info();
            ASSERT_EQ(2, partInfo.part_count());
            ASSERT_EQ(1, partInfo.part_ids_size());
            ASSERT_EQ(1, partInfo.part_ids(0));
        }
    }
    {
        const auto &userGraph = graphDef->sub_graphs(1);
        ASSERT_EQ("navi.user_biz", userGraph.location().biz_name());
        ASSERT_EQ(1, userGraph.borders_size());
        const auto &border = userGraph.borders(0);
        // biz_qrs --> user_biz
        ASSERT_EQ(0, border.peer().graph_id());
        const auto &partInfo = border.peer().part_info();
        ASSERT_EQ(3, partInfo.part_count());
        ASSERT_EQ(3, partInfo.part_ids_size());
    }
    {
        const auto &bizAGraph = graphDef->sub_graphs(2);
        ASSERT_EQ("biz_a", bizAGraph.location().biz_name());
        ASSERT_EQ(1, bizAGraph.borders_size());
        const auto &border = bizAGraph.borders(0);
        // biz_a --> biz_qrs
        ASSERT_EQ(0, border.peer().graph_id());
        const auto &partInfo = border.peer().part_info();
        ASSERT_EQ(3, partInfo.part_count());
        ASSERT_EQ(3, partInfo.part_ids_size());
    }

}

void PartAccessGraphTest::buildSimpleGraph(GraphDef *graphDef) {
    GraphBuilder builder(graphDef);
    auto mainGraph = builder.newSubGraph("biz_qrs");
    builder.subGraph(mainGraph).partIds({2});
    auto identityQrs = builder.node("identity_qrs").kernel("IdentityTestKernel");

    auto aBizGraph1 = builder.newSubGraph("biz_a");
    builder.subGraph(aBizGraph1).partIds({1});
    auto identityA = builder.node("identity_a_1").kernel("IdentityTestKernel");
    identityA.out("output1").to(identityQrs.in("input1"));

    auto bBizGraph = builder.newSubGraph("biz_b");
    builder.subGraph(bBizGraph).partIds({1, 2});
    auto identityB = builder.node("identity_b").kernel("IdentityTestKernel");
    identityB.out("output1").to(identityA.in("input1"));

    auto aBizGraph2 = builder.newSubGraph("biz_a");
    builder.subGraph(aBizGraph2).partIds({1});
    builder.node("source_a_2")
        .kernel("SourceKernel")
        .jsonAttrs(R"json({"times" : 1})json")
        .out("output1")
        .to(identityB.in("input1"));

    ASSERT_TRUE(builder.ok());
}

TEST_F(PartAccessGraphTest, testSimple)
{
    NaviTestCluster cluster;
    ASSERT_NO_FATAL_FAILURE(buildCluster(cluster));

    ASSERT_NO_FATAL_FAILURE(buildSimpleGraph(_graphDef.get()));

    GraphBuilder builder(_graphDef.get());
    builder.node(0, "identity_qrs").out("output1").asGraphOutput("GraphOutput1");

    RunGraphParams params;
    params.setTimeoutMs(10000000);
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
    auto naviResult = userResult->getNaviResult();
    naviResult->show();

    EXPECT_EQ("EC_NONE", std::string(CommonUtil::getErrorString(naviResult->ec)));
    EXPECT_EQ("", naviResult->errorEvent.message);

    // qrs * biz_a * biz_b * biz_a * qrs
    // 1 * 1 * 2 * 1 * 1 = 2
    ASSERT_EQ(2, dataVec.size());

    auto &rpcInfoMap = naviResult->rpcInfoMap;
    ASSERT_NO_FATAL_FAILURE(sortRpcInfoMap(rpcInfoMap));
    {
        auto it = rpcInfoMap.find(make_pair("(-1)", "biz_a"));
        ASSERT_TRUE(it != rpcInfoMap.end());
        auto &rpcVec = it->second;
        ASSERT_EQ(2, rpcVec.size());
        ASSERT_FALSE(rpcVec[0].isLack());
        ASSERT_EQ(1, rpcVec[0].partId);
        ASSERT_FALSE(rpcVec[1].isLack());
        ASSERT_EQ(1, rpcVec[1].partId);
    }
    {
        auto it = rpcInfoMap.find(make_pair("(-1)", "biz_b"));
        ASSERT_TRUE(it != rpcInfoMap.end());
        auto &rpcVec = it->second;
        ASSERT_EQ(2, rpcVec.size());
        ASSERT_FALSE(rpcVec[0].isLack());
        ASSERT_EQ(1, rpcVec[0].partId);
        ASSERT_FALSE(rpcVec[1].isLack());
        ASSERT_EQ(2, rpcVec[1].partId);
    }
    {
        auto it = rpcInfoMap.find(make_pair("(-1)", "biz_qrs"));
        ASSERT_TRUE(it != rpcInfoMap.end());
        auto &rpcVec = it->second;
        ASSERT_EQ(1, rpcVec.size());
        ASSERT_FALSE(rpcVec[0].isLack());
        ASSERT_EQ(2, rpcVec[0].partId);
    }
}


TEST_F(PartAccessGraphTest, testRunForkGraph)
{
    NaviTestCluster cluster;
    ASSERT_NO_FATAL_FAILURE(buildCluster(cluster));

    GraphDef forkGraphDef;
    ASSERT_NO_FATAL_FAILURE(buildSimpleGraph(&forkGraphDef));
    std::string forkGraphStr;
    ASSERT_TRUE(forkGraphDef.SerializeToString(&forkGraphStr));
    GraphBuilder builder(_graphDef.get());
    auto mainGraph = builder.newSubGraph("biz_qrs");
    builder.subGraph(mainGraph).partIds({1});
    builder.node("fork_graph_node")
        .kernel("ForkGraphKernel")
        .attr("output_node_name", "identity_qrs")
        .attr("output_node_port", "output1")
        .attr("fork_graph", forkGraphStr)
        .out("output1").asGraphOutput("GraphOutput1");

    ASSERT_TRUE(builder.ok());

    std::cout << "aaaaaa: " << forkGraphDef.DebugString() << std::endl;
    RunGraphParams params;
    params.setTimeoutMs(10000000);
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
    auto naviResult = userResult->getNaviResult();

    EXPECT_EQ("EC_NONE", std::string(CommonUtil::getErrorString(naviResult->ec)));
    EXPECT_EQ("", naviResult->errorEvent.message);

    naviResult->show();

    // qrs(fork_node) * biz_a * biz_b * biz_a * qrs
    // 1 * 1 * 2 * 1 * 1 = 2
    EXPECT_EQ(2, dataVec.size());

    auto &rpcInfoMap = naviResult->rpcInfoMap;
    ASSERT_NO_FATAL_FAILURE(sortRpcInfoMap(rpcInfoMap));
    {
        auto it = rpcInfoMap.find(make_pair("biz_qrs(1)", "biz_a"));
        ASSERT_TRUE(it != rpcInfoMap.end());
        auto &rpcVec = it->second;
        ASSERT_EQ(2, rpcVec.size());
        ASSERT_FALSE(rpcVec[0].isLack());
        ASSERT_EQ(1, rpcVec[0].partId);
        ASSERT_FALSE(rpcVec[1].isLack());
        ASSERT_EQ(1, rpcVec[1].partId);
    }
    {
        auto it = rpcInfoMap.find(make_pair("biz_qrs(1)", "biz_b"));
        ASSERT_TRUE(it != rpcInfoMap.end());
        auto &rpcVec = it->second;
        ASSERT_EQ(2, rpcVec.size());
        ASSERT_FALSE(rpcVec[0].isLack());
        ASSERT_EQ(1, rpcVec[0].partId);
        ASSERT_FALSE(rpcVec[1].isLack());
        ASSERT_EQ(2, rpcVec[1].partId);
    }
    {
        auto it = rpcInfoMap.find(make_pair("biz_qrs(1)", "biz_qrs"));
        ASSERT_TRUE(it != rpcInfoMap.end());
        auto &rpcVec = it->second;
        ASSERT_EQ(1, rpcVec.size());
        ASSERT_FALSE(rpcVec[0].isLack());
        ASSERT_EQ(2, rpcVec[0].partId);
    }
}

TEST_F(PartAccessGraphTest, testBizA2BizQ_Error_PartIdOverflow)
{
    NaviTestCluster cluster;
    ASSERT_NO_FATAL_FAILURE(buildCluster(cluster));

    GraphBuilder builder(_graphDef.get());

    builder.newSubGraph("biz_qrs");
    auto identityQrs = builder.node("identity_qrs").kernel("IdentityTestKernel");
    identityQrs.out("output1").asGraphOutput("GraphOutput1");

    auto aBizGraph = builder.newSubGraph("biz_a");
    builder.subGraph(aBizGraph).partIds({1, 2});
    builder.node("source_a")
        .kernel("SourceKernel")
        .jsonAttrs(R"json({"times" : 2})json")
        .out("output1")
        .to(identityQrs.in("input1"));

    ASSERT_TRUE(builder.ok());

    RunGraphParams params;
    params.setTimeoutMs(10000000);

    auto navi = cluster.getNavi("host_0");
    WaitClosure waitClosure;
    NaviFuncClosure closure([&waitClosure](NaviUserResultPtr naviUserResult) {
                                autil::ScopeGuard guard([&waitClosure]() { waitClosure.notify(); });
                                ASSERT_NE(nullptr, naviUserResult);
                                auto naviResult = naviUserResult->getNaviResult();
                                ASSERT_EQ(EC_INIT_GRAPH, naviResult->ec) << naviResult->errorEvent.message;
                                ASSERT_EQ("init gig stream failed, biz: biz_a", naviResult->errorEvent.message);
                            });
    navi->runLocalGraphAsync(_graphDef.release(), params, {}, &closure);
    waitClosure.wait();
}

TEST_F(PartAccessGraphTest, testRunForkGraphOnLocalBiz)
{
    NaviTestCluster cluster;
    ASSERT_NO_FATAL_FAILURE(buildCluster(cluster));

    GraphDef forkGraphDef;
    ASSERT_NO_FATAL_FAILURE(buildSimpleGraph(&forkGraphDef));
    std::string forkGraphStr;
    ASSERT_TRUE(forkGraphDef.SerializeToString(&forkGraphStr));
    GraphBuilder builder(_graphDef.get());
    auto mainGraph = builder.newSubGraph("biz_qrs");
    builder.subGraph(mainGraph).partIds({1});
    builder.node("fork_graph_node")
        .kernel("ForkGraphKernel")
        .attr("output_node_name", "identity_qrs")
        .attr("output_node_port", "output1")
        .attr("fork_graph", forkGraphStr)
        .attr("local_biz_name", "biz_qrs")
        .out("output1")
        .asGraphOutput("GraphOutput1");

    ASSERT_TRUE(builder.ok());

    RunGraphParams params;
    params.setTimeoutMs(10000000);
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
    auto naviResult = userResult->getNaviResult();

    EXPECT_EQ("EC_NONE", std::string(CommonUtil::getErrorString(naviResult->ec)));
    EXPECT_EQ("", naviResult->errorEvent.message);

    naviResult->show();

    // qrs(fork_node) * biz_a * biz_b * biz_a * qrs
    // 1 * 1 * 2 * 1 * 1 = 2
    EXPECT_EQ(2, dataVec.size());

    auto &rpcInfoMap = naviResult->rpcInfoMap;
    ASSERT_NO_FATAL_FAILURE(sortRpcInfoMap(rpcInfoMap));
    {
        auto it = rpcInfoMap.find(make_pair("biz_qrs(1)", "biz_a"));
        ASSERT_TRUE(it != rpcInfoMap.end());
        auto &rpcVec = it->second;
        ASSERT_EQ(2, rpcVec.size());
        ASSERT_FALSE(rpcVec[0].isLack());
        ASSERT_EQ(1, rpcVec[0].partId);
        ASSERT_FALSE(rpcVec[1].isLack());
        ASSERT_EQ(1, rpcVec[1].partId);
    }
    {
        auto it = rpcInfoMap.find(make_pair("biz_qrs(1)", "biz_b"));
        ASSERT_TRUE(it != rpcInfoMap.end());
        auto &rpcVec = it->second;
        ASSERT_EQ(2, rpcVec.size());
        ASSERT_FALSE(rpcVec[0].isLack());
        ASSERT_EQ(1, rpcVec[0].partId);
        ASSERT_FALSE(rpcVec[1].isLack());
        ASSERT_EQ(2, rpcVec[1].partId);
    }
    {
        auto it = rpcInfoMap.find(make_pair("biz_qrs(1)", "biz_qrs"));
        ASSERT_TRUE(it == rpcInfoMap.end()) << "fork node should create biz_qrs as local domain";
    }
}

TEST_F(PartAccessGraphTest, testRunForkGraphWithOverride)
{
    NaviTestCluster cluster;
    ASSERT_NO_FATAL_FAILURE(buildCluster(cluster));

    GraphBuilder builder(_graphDef.get());
    auto mainGraph = builder.newSubGraph("biz_qrs");
    builder.subGraph(mainGraph).partIds({1});
    auto n = builder.node("fork_graph_node")
                 .kernel("ForkGraphWithOverrideKernel")
                 .attr("biz_name", "biz_a")
                 .integerAttr("part_count", 2)
                 .integerAttr("select_index", 4)
                 .jsonAttrs(R"json({"r5" : { "r5_key" : "r5_value"}})json");
    auto in = n.in("input1");
    auto in0 = in.autoNext();
    auto in1 = in0.autoNext();
    builder.node("source_0").kernel("SourceKernel").jsonAttrs(R"json({"times" : 2})json").out("output1").to(in0);
    builder.node("source_1").kernel("SourceKernel").jsonAttrs(R"json({"times" : 2})json").out("output1").to(in1);

    auto out = n.out("output1");
    auto out0 = out.autoNext();
    auto out1 = out0.autoNext();
    auto out2 = out1.autoNext();
    out0.asGraphOutput("GraphOutput1");
    out1.asGraphOutput("GraphOutput2");
    out2.asGraphOutput("GraphOutput3");

    ASSERT_TRUE(builder.ok());

    RunGraphParams params;
    params.setTimeoutMs(10000000);
    // params.setTraceLevel("SCHEDULE2");

    auto navi = cluster.getNavi("host_0");

    auto userResult = navi->runGraph(_graphDef.release(), params);

    std::vector<NaviUserData> dataVec;
    while (true) {
        NaviUserData data;
        bool eof = false;
        if (userResult->nextData(data, eof) && data.data) {
            if (data.data) {
                std::cout << data.name << ", part " << data.partId << ": ";
                data.data->show();
            }
            dataVec.push_back(data);
        }
        if (eof) {
            break;
        }
    }
    auto naviResult = userResult->getNaviResult();

    EXPECT_EQ("EC_NONE", std::string(CommonUtil::getErrorString(naviResult->ec)));
    EXPECT_EQ("", naviResult->errorEvent.message);

    naviResult->show();

    EXPECT_EQ(10, dataVec.size());
}

TEST_F(PartAccessGraphTest, testResourceOverride1)
{
    NaviTestCluster cluster;
    ASSERT_NO_FATAL_FAILURE(buildCluster(cluster));

    GraphBuilder builder(_graphDef.get());
    auto mainGraph = builder.newSubGraph("biz_qrs");
    builder.subGraph(mainGraph).partIds({1});
    auto n = builder.node("resource_override_node")
                 .kernel("TestResourceOverride")
                 .attr("biz_name", "biz_a");
    builder.replaceR("A", "ReplaceAA");
    builder.replaceR("ReplaceA", "ReplaceAA");
    n.out("output1").asGraphOutput("GraphOutput1");

    ASSERT_TRUE(builder.ok());

    RunGraphParams params;
    params.setTimeoutMs(10000000);
    // params.setTraceLevel("SCHEDULE2");

    auto navi = cluster.getNavi("host_0");

    auto userResult = navi->runGraph(_graphDef.release(), params);

    std::vector<NaviUserData> dataVec;
    while (true) {
        NaviUserData data;
        bool eof = false;
        if (userResult->nextData(data, eof) && data.data) {
            if (data.data) {
                std::cout << data.name << ", part " << data.partId << ": ";
                data.data->show();
            }
            dataVec.push_back(data);
        }
        if (eof) {
            break;
        }
    }
    auto naviResult = userResult->getNaviResult();

    EXPECT_EQ("EC_NONE", std::string(CommonUtil::getErrorString(naviResult->ec)));
    EXPECT_EQ("", naviResult->errorEvent.message);

    naviResult->show();

    EXPECT_EQ(9, dataVec.size());
}

TEST_F(PartAccessGraphTest, testResourceOverride2)
{
    NaviTestCluster cluster;
    ASSERT_NO_FATAL_FAILURE(buildCluster(cluster));

    GraphBuilder builder(_graphDef.get());
    auto mainGraph = builder.newSubGraph("biz_qrs");
    builder.subGraph(mainGraph).partIds({1});
    auto n = builder.node("resource_override_node")
                 .kernel("TestResourceOverride")
                 .attr("biz_name", "biz_a");
    builder.replaceR("ReplaceA", "ReplaceAA");
    builder.replaceR("A", "ReplaceA");
    n.out("output1").asGraphOutput("GraphOutput1");

    ASSERT_TRUE(builder.ok());

    RunGraphParams params;
    params.setTimeoutMs(10000000);
    // params.setTraceLevel("SCHEDULE2");

    auto navi = cluster.getNavi("host_0");

    auto userResult = navi->runGraph(_graphDef.release(), params);

    std::vector<NaviUserData> dataVec;
    while (true) {
        NaviUserData data;
        bool eof = false;
        if (userResult->nextData(data, eof) && data.data) {
            if (data.data) {
                std::cout << data.name << ", part " << data.partId << ": ";
                data.data->show();
            }
            dataVec.push_back(data);
        }
        if (eof) {
            break;
        }
    }
    auto naviResult = userResult->getNaviResult();

    EXPECT_EQ("EC_NONE", std::string(CommonUtil::getErrorString(naviResult->ec)));
    EXPECT_EQ("", naviResult->errorEvent.message);

    naviResult->show();

    EXPECT_EQ(9, dataVec.size());
}

TEST_F(PartAccessGraphTest, testResourceOverride3)
{
    NaviTestCluster cluster;
    ASSERT_NO_FATAL_FAILURE(buildCluster(cluster));

    GraphBuilder builder(_graphDef.get());
    auto mainGraph = builder.newSubGraph("biz_qrs");
    builder.subGraph(mainGraph).partIds({1});
    auto n = builder.node("resource_override_node")
                 .kernel("TestResourceOverride")
                 .attr("biz_name", "biz_a");
    builder.replaceR("A", "ReplaceA");
    builder.replaceR("ReplaceA", "ReplaceAA");
    n.out("output1").asGraphOutput("GraphOutput1");

    ASSERT_TRUE(builder.ok());

    RunGraphParams params;
    params.setTimeoutMs(10000000);
    // params.setTraceLevel("SCHEDULE2");

    auto navi = cluster.getNavi("host_0");

    auto userResult = navi->runGraph(_graphDef.release(), params);

    std::vector<NaviUserData> dataVec;
    while (true) {
        NaviUserData data;
        bool eof = false;
        if (userResult->nextData(data, eof) && data.data) {
            if (data.data) {
                std::cout << data.name << ", part " << data.partId << ": ";
                data.data->show();
            }
            dataVec.push_back(data);
        }
        if (eof) {
            break;
        }
    }
    auto naviResult = userResult->getNaviResult();

    EXPECT_EQ("EC_NONE", std::string(CommonUtil::getErrorString(naviResult->ec)));
    EXPECT_EQ("", naviResult->errorEvent.message);

    naviResult->show();

    EXPECT_EQ(9, dataVec.size());
}

TEST_F(PartAccessGraphTest, testResourceOverride4)
{
    NaviTestCluster cluster;
    ASSERT_NO_FATAL_FAILURE(buildCluster(cluster));

    GraphBuilder builder(_graphDef.get());
    auto mainGraph = builder.newSubGraph("biz_qrs");
    builder.subGraph(mainGraph).partIds({1});
    auto n = builder.node("resource_override_node")
                 .kernel("TestResourceOverride2")
                 .attr("biz_name", "biz_a");
    builder.replaceR("B", "ReplaceB");
    n.out("output1").asGraphOutput("GraphOutput1");

    ASSERT_TRUE(builder.ok());

    RunGraphParams params;
    params.setTimeoutMs(10000000);
    // params.setTraceLevel("SCHEDULE2");

    auto navi = cluster.getNavi("host_0");

    auto userResult = navi->runGraph(_graphDef.release(), params);

    std::vector<NaviUserData> dataVec;
    while (true) {
        NaviUserData data;
        bool eof = false;
        if (userResult->nextData(data, eof) && data.data) {
            if (data.data) {
                std::cout << data.name << ", part " << data.partId << ": ";
                data.data->show();
            }
            dataVec.push_back(data);
        }
        if (eof) {
            break;
        }
    }
    auto naviResult = userResult->getNaviResult();

    EXPECT_EQ("EC_NONE", std::string(CommonUtil::getErrorString(naviResult->ec)));
    EXPECT_EQ("", naviResult->errorEvent.message);

    naviResult->show();

    EXPECT_EQ(9, dataVec.size());
}

TEST_F(PartAccessGraphTest, testGraphTimeout)
{
    NaviTestCluster cluster;
    ASSERT_NO_FATAL_FAILURE(buildCluster(cluster));

    GraphBuilder builder(_graphDef.get());
    auto mainGraph = builder.newSubGraph("biz_qrs");
    builder.subGraph(mainGraph).partIds({0});
    auto source = builder.node("source_node_1").kernel("SourceKernel").jsonAttrs(R"json({"times" : 10})json");
    auto sleep = builder.node("sleep").kernel("SleepKernel");
    sleep.in("input1").from(source.out("output1"));
    sleep.out("output1").asGraphOutput("graph_output");
    ASSERT_TRUE(builder.ok());

    RunGraphParams params;
    params.setTimeoutMs(100);
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
    auto naviResult = userResult->getNaviResult();

    EXPECT_EQ("EC_TIMEOUT", std::string(CommonUtil::getErrorString(naviResult->ec)));
    EXPECT_EQ("session timeout", naviResult->errorEvent.message);
}

} // namespace navi
