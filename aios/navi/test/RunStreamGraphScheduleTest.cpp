#include "autil/EnvUtil.h"
#include "autil/Scope.h"
#include "autil/Thread.h"
#include "navi/builder/GraphBuilder.h"
#include "navi/engine/NaviSnapshot.h"
#include "navi/engine/Node.h"
#include "navi/example/TestData.h"
#include "navi/example/TestKernel.h"
#include "navi/example/TestResource.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/test_cluster/NaviFuncClosure.h"
#include "navi/test_cluster/NaviGraphRunner.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "navi/tester/TesterDef.h"
#include "navi/util/NaviClosure.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;

namespace navi {

class RunStreamGraphScheduleTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

private:
    void buildCluster(NaviTestCluster &cluster);
    void buildExampleGraph(std::unique_ptr<GraphDef> &graphDef);
    void buildExampleGraph2(std::unique_ptr<GraphDef> &graphDef);
};

void RunStreamGraphScheduleTest::setUp() {}
void RunStreamGraphScheduleTest::tearDown() {}

void RunStreamGraphScheduleTest::buildCluster(NaviTestCluster &cluster) {
    autil::EnvGuard _("NAVI_PYTHON_HOME", "./aios/navi/config_loader/python:/usr/lib64/python3.6/");
    std::string loader = "./aios/navi/testdata/test_config_loader.py";
    ResourceMapPtr rootResourceMap(new ResourceMap());
    std::string configPath = "./aios/navi/testdata/config/run_graph_test";
    std::string biz1Config = "./aios/navi/testdata/config/run_graph_test/biz1.py";

    ASSERT_TRUE(cluster.addServer("host_0", loader, configPath, rootResourceMap));
    ASSERT_TRUE(cluster.addServer("host_1", loader, configPath, rootResourceMap));
    ASSERT_TRUE(cluster.addServer("host_2", loader, configPath, rootResourceMap));

    ASSERT_TRUE(cluster.addBiz("host_0", "biz_a", 1, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_1", "biz_b", 1, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_1", "biz_c", 1, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_1", "biz_d", 1, 0, biz1Config));
    ASSERT_TRUE(cluster.addBiz("host_2", "biz_e", 1, 0, biz1Config));

    ASSERT_TRUE(cluster.start());
}

void RunStreamGraphScheduleTest::buildExampleGraph(std::unique_ptr<GraphDef> &graphDef) {
    graphDef = std::make_unique<GraphDef>();
    GraphBuilder builder(graphDef.get());
    builder.newSubGraph("biz_b");
    auto sb = builder.node("sb").kernel("SourceKernel").jsonAttrs(R"json({"sleep_ms" : 1000, "times" : 1})json");
    builder.newSubGraph("biz_c");
    auto sc = builder.node("sc").kernel("SourceKernel").jsonAttrs(R"json({"sleep_ms" : 1000, "times" : 1})json");
    builder.newSubGraph("biz_d");
    auto sd = builder.node("sd").kernel("SourceKernel").jsonAttrs(R"json({"sleep_ms" : 1000, "times" : 1})json");

    builder.newSubGraph("biz_a");
    auto sink1 = builder.node("sink1").kernel("MergeKernel");
    sink1.in("input1").from(sb.out("output1"));
    sink1.in("input2").from(sc.out("output1"));
    auto sink2 = builder.node("sink2").kernel("MergeKernel");
    sink2.in("input1").from(sink1.out("output1"));
    sink2.in("input2").from(sd.out("output1"));
    sink2.out("output1").asGraphOutput("o");
    ASSERT_TRUE(builder.ok());
}

void RunStreamGraphScheduleTest::buildExampleGraph2(std::unique_ptr<GraphDef> &graphDef) {
    graphDef = std::make_unique<GraphDef>();
    GraphBuilder builder(graphDef.get());
    builder.newSubGraph("biz_b");
    auto sb = builder.node("sb").kernel("SourceKernel").jsonAttrs(R"json({"sleep_ms" : 1000, "times" : 1})json");
    builder.newSubGraph("biz_e");
    auto se = builder.node("se").kernel("SourceKernel").jsonAttrs(R"json({"sleep_ms" : 1000, "times" : 1})json");
    builder.newSubGraph("biz_a");
    auto sink = builder.node("sink").kernel("MergeKernel");
    sink.in("input1").from(sb.out("output1"));
    sink.in("input2").from(se.out("output1"));
    sink.out("output1").asGraphOutput("o");
    ASSERT_TRUE(builder.ok());
}

TEST_F(RunStreamGraphScheduleTest, testOneProcessingMax) {
    WaitClosure waitClosure;

    NaviTestCluster cluster;
    ASSERT_NO_FATAL_FAILURE(buildCluster(cluster));
    std::unique_ptr<GraphDef> graphDef;
    ASSERT_NO_FATAL_FAILURE(buildExampleGraph(graphDef));

    auto navi = cluster.getNavi("host_0");
    ASSERT_TRUE(navi);
    RunGraphParams params;
    params.setTimeoutMs(100000);
    params.setTaskQueueName("slow_queue");
    // params.setTraceLevel("SCHEDULE1");
    NaviFuncClosure closure([&waitClosure](NaviUserResultPtr naviUserResult) {
        autil::ScopeGuard guard([&waitClosure]() { waitClosure.notify(); });
        ASSERT_NE(nullptr, naviUserResult);
        auto naviResult = naviUserResult->getNaviResult();
        ASSERT_EQ(EC_NONE, naviResult->getErrorCode()) << naviResult->getErrorMessage();

        // std::vector<std::string> traceVec;
        // naviResult->collectTrace(traceVec);

        NaviUserData data;
        bool eof = false;
        naviUserResult->nextData(data, eof);
        ASSERT_TRUE(eof);
        auto *typedData = dynamic_cast<HelloData *>(data.data.get());
        ASSERT_TRUE(typedData);
        ASSERT_THAT(typedData->_dataVec, ElementsAre("2_sb_1", "2_sc_1", "2_sd_1"));
    });
    navi->runLocalGraphAsync(graphDef.release(), params, {}, &closure);
    waitClosure.wait();
    ASSERT_FALSE(HasFatalFailure());
}

TEST_F(RunStreamGraphScheduleTest, testScheduleQueueExceed) {
    WaitClosure waitClosure;

    NaviTestCluster cluster;
    ASSERT_NO_FATAL_FAILURE(buildCluster(cluster));
    std::unique_ptr<GraphDef> graphDef;
    ASSERT_NO_FATAL_FAILURE(buildExampleGraph(graphDef));

    auto navi = cluster.getNavi("host_0");
    ASSERT_TRUE(navi);
    RunGraphParams params;
    params.setTimeoutMs(100000);
    params.setTaskQueueName("slow_slow_queue");
    // params.setTraceLevel("SCHEDULE1");
    NaviFuncClosure closure([&waitClosure](NaviUserResultPtr naviUserResult) {
        autil::ScopeGuard guard([&waitClosure]() { waitClosure.notify(); });
        ASSERT_NE(nullptr, naviUserResult);
        auto naviResult = naviUserResult->getNaviResult();
        ASSERT_NE(EC_NONE, naviResult->getErrorCode());
    });
    navi->runLocalGraphAsync(graphDef.release(), params, {}, &closure);
    waitClosure.wait();
    ASSERT_FALSE(HasFatalFailure());
}

TEST_F(RunStreamGraphScheduleTest, testProcessingWithQueueMax) {
    WaitClosure waitClosure;

    NaviTestCluster cluster;
    ASSERT_NO_FATAL_FAILURE(buildCluster(cluster));
    std::unique_ptr<GraphDef> graphDef;
    ASSERT_NO_FATAL_FAILURE(buildExampleGraph2(graphDef));

    auto navi = cluster.getNavi("host_0");
    ASSERT_TRUE(navi);
    RunGraphParams params;
    params.setTimeoutMs(100000);
    params.setTaskQueueName("slow_slow_slow_queue");
    NaviFuncClosure closure([&waitClosure](NaviUserResultPtr naviUserResult) {
        autil::ScopeGuard guard([&waitClosure]() { waitClosure.notify(); });
        ASSERT_NE(nullptr, naviUserResult);
        auto naviResult = naviUserResult->getNaviResult();
        ASSERT_EQ(EC_NONE, naviResult->getErrorCode()) << naviResult->getErrorMessage();

        NaviUserData data;
        bool eof = false;
        naviUserResult->nextData(data, eof);
        ASSERT_TRUE(eof);
        auto *typedData = dynamic_cast<HelloData *>(data.data.get());
        ASSERT_TRUE(typedData);
        ASSERT_THAT(typedData->_dataVec, ElementsAre("2_sb_1", "2_se_1"));
    });
    navi->runLocalGraphAsync(graphDef.release(), params, {}, &closure);
    waitClosure.wait();
    ASSERT_FALSE(HasFatalFailure());
}

} // namespace navi
