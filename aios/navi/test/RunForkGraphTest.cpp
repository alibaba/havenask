#include "autil/Thread.h"
#include "autil/Scope.h"
#include "navi/builder/GraphBuilder.h"
#include "navi/engine/NaviSnapshot.h"
#include "navi/engine/Node.h"
#include "navi/example/TestData.h"
#include "navi/example/TestKernel.h"
#include "navi/example/TestResource.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/test_cluster/NaviGraphRunner.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "navi/tester/TesterDef.h"
#include "navi/util/NaviClosure.h"
#include "navi/test_cluster/NaviFuncClosure.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;

namespace navi {

class RunForkGraphTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void RunForkGraphTest::setUp() {}

void RunForkGraphTest::tearDown() {}

TEST_F(RunForkGraphTest, testRunForkGraph_withOptional) {
    NaviGraphRunner naviGraphRunner;
    ASSERT_TRUE(naviGraphRunner.init());
    
    auto graphDef = std::make_unique<GraphDef>();
    GraphBuilder builder(graphDef.get());
    {
        std::string forkGraphStr;
        GraphDef forkGraphDef;
        GraphBuilder forkGraphBuilder(&forkGraphDef);
        forkGraphBuilder.newSubGraph(naviGraphRunner.getBizName());
        forkGraphBuilder.node("inner_source")
            .kernel("SourceKernel")
            .jsonAttrs(R"json({"times" : 1, "sleep_ms": 1000})json");
        ASSERT_TRUE(forkGraphBuilder.ok());
        ASSERT_TRUE(forkGraphDef.SerializeToString(&forkGraphStr));

        builder.newSubGraph(naviGraphRunner.getBizName());
        auto right = builder.node("right").kernel("SourceKernel").jsonAttrs(R"json({"times" : 0})json");
        auto left =
            builder.node("left")
                .kernel("ForkGraphKernel")
                .jsonAttrs(R"json({"output_node_name": "inner_source", "output_node_port": "output1"})json")
                .binaryAttrsFromMap({{"fork_graph", forkGraphStr}, {"local_biz_name", naviGraphRunner.getBizName()}});
        auto downstream = builder.node("downstream").kernel("FakeUnionKernel");
        right.out("output1").to(downstream.in("input1:1")); // DO NOT change order
        left.out("output1").to(downstream.in("input1:0"));  // DO NOT change order
        downstream.out("output1").asGraphOutput("o");
    }

    ASSERT_TRUE(builder.ok());
    WaitClosure waitClosure;
    NaviFuncClosure closure([&waitClosure](NaviUserResultPtr naviUserResult) {
        autil::ScopeGuard guard([&waitClosure]() { waitClosure.notify(); });
        ASSERT_NE(nullptr, naviUserResult);
        auto naviResult = naviUserResult->getNaviResult();
        ASSERT_NE(nullptr, naviUserResult);
        ASSERT_EQ(EC_NONE, naviResult->getErrorCode()) << naviResult->getErrorMessage();
        NaviUserData data;
        bool eof = false;
        ASSERT_TRUE(naviUserResult->nextData(data, eof));
        ASSERT_TRUE(eof);
        ASSERT_EQ(nullptr, data.data);
    });
    // NaviLoggerProvider logProvider("SCHEDULE2");
    naviGraphRunner.runLocalGraphAsync(graphDef.release(), {}, &closure);
    waitClosure.wait();
    ASSERT_FALSE(HasFatalFailure());
}

TEST_F(RunForkGraphTest, testRunForkGraph_forkGraphTimeoutAsEof) {
    NaviGraphRunner naviGraphRunner;
    ASSERT_TRUE(naviGraphRunner.init());

    auto graphDef = std::make_unique<GraphDef>();
    GraphBuilder builder(graphDef.get());
    {
        std::string forkGraphStr;
        GraphDef forkGraphDef;
        GraphBuilder forkGraphBuilder(&forkGraphDef);
        forkGraphBuilder.newSubGraph(naviGraphRunner.getBizName());
        forkGraphBuilder.node("inner_source")
            .kernel("SourceKernel")
            .jsonAttrs(R"json({"times" : 1, "sleep_ms": 1000})json");
        ASSERT_TRUE(forkGraphBuilder.ok());
        ASSERT_TRUE(forkGraphDef.SerializeToString(&forkGraphStr));

        builder.newSubGraph(naviGraphRunner.getBizName());
        auto left =
            builder.node("fork_node")
                .kernel("ForkGraphKernel")
                .jsonAttrs(
                    R"json({
			"output_node_name": "inner_source",
			"output_node_port": "output1",
			"timeout_ms": 1
		    })json")
                .binaryAttrsFromMap({{"fork_graph", forkGraphStr}, {"local_biz_name", naviGraphRunner.getBizName()}});
        auto downstream = builder.node("downstream").kernel("IdentityTestKernel");
        left.out("output1").to(downstream.in("input1"));
        downstream.out("output1").asGraphOutput("o");
    }
    ASSERT_TRUE(builder.ok());
    WaitClosure waitClosure;
    NaviFuncClosure closure([&waitClosure](NaviUserResultPtr naviUserResult) {
        autil::ScopeGuard guard([&waitClosure]() { waitClosure.notify(); });
        ASSERT_NE(nullptr, naviUserResult);
        auto naviResult = naviUserResult->getNaviResult();
        ASSERT_NE(nullptr, naviUserResult);
        ASSERT_EQ(EC_NONE, naviResult->getErrorCode()) << naviResult->getErrorMessage();
        NaviUserData data;
        bool eof = false;
        ASSERT_TRUE(naviUserResult->nextData(data, eof));
        ASSERT_TRUE(eof);
        ASSERT_EQ(nullptr, data.data);
    });
    // NaviLoggerProvider logProvider("SCHEDULE1");
    naviGraphRunner.runLocalGraphAsync(graphDef.release(), {}, &closure);
    waitClosure.wait();
    ASSERT_FALSE(HasFatalFailure());
}

TEST_F(RunForkGraphTest, testRunForkGraph_forkGraphTimeoutAsError) {
    NaviGraphRunner naviGraphRunner;
    ASSERT_TRUE(naviGraphRunner.init());

    auto graphDef = std::make_unique<GraphDef>();
    GraphBuilder builder(graphDef.get());
    {
        std::string forkGraphStr;
        GraphDef forkGraphDef;
        GraphBuilder forkGraphBuilder(&forkGraphDef);
        forkGraphBuilder.newSubGraph(naviGraphRunner.getBizName());
        forkGraphBuilder.node("inner_source")
            .kernel("SourceKernel")
            .jsonAttrs(R"json({"times" : 1, "sleep_ms": 1000})json");
        ASSERT_TRUE(forkGraphBuilder.ok());
        ASSERT_TRUE(forkGraphDef.SerializeToString(&forkGraphStr));

        builder.newSubGraph(naviGraphRunner.getBizName());
        auto left =
            builder.node("fork_node")
                .kernel("ForkGraphKernel")
                .jsonAttrs(
                    R"json({
			"output_node_name": "inner_source",
			"output_node_port": "output1",
			"timeout_ms": 1,
			"error_as_eof": false
		    })json")
                .binaryAttrsFromMap({{"fork_graph", forkGraphStr}, {"local_biz_name", naviGraphRunner.getBizName()}});
        auto downstream = builder.node("downstream").kernel("IdentityTestKernel");
        left.out("output1").to(downstream.in("input1"));
        downstream.out("output1").asGraphOutput("o");
    }
    ASSERT_TRUE(builder.ok());
    WaitClosure waitClosure;
    NaviFuncClosure closure([&waitClosure](NaviUserResultPtr naviUserResult) {
        autil::ScopeGuard guard([&waitClosure]() { waitClosure.notify(); });
        ASSERT_NE(nullptr, naviUserResult);
        auto naviResult = naviUserResult->getNaviResult();
        ASSERT_NE(nullptr, naviUserResult);
        ASSERT_EQ(EC_TIMEOUT, naviResult->getErrorCode()) << naviResult->getErrorMessage();
    });
    // NaviLoggerProvider logProvider("SCHEDULE1");
    naviGraphRunner.runLocalGraphAsync(graphDef.release(), {}, &closure);
    waitClosure.wait();
    ASSERT_FALSE(HasFatalFailure());
}

TEST_F(RunForkGraphTest, testRunForkGraph_sessionTimeout) {
    NaviGraphRunner naviGraphRunner;
    ASSERT_TRUE(naviGraphRunner.init());

    auto graphDef = std::make_unique<GraphDef>();
    GraphBuilder builder(graphDef.get());
    {
        std::string forkGraphStr;
        GraphDef forkGraphDef;
        GraphBuilder forkGraphBuilder(&forkGraphDef);
        forkGraphBuilder.newSubGraph(naviGraphRunner.getBizName());
        forkGraphBuilder.node("inner_source")
            .kernel("SourceKernel")
            .jsonAttrs(R"json({"times" : 1, "sleep_ms": 1000})json");
        ASSERT_TRUE(forkGraphBuilder.ok());
        ASSERT_TRUE(forkGraphDef.SerializeToString(&forkGraphStr));

        builder.newSubGraph(naviGraphRunner.getBizName());
        auto left =
            builder.node("fork_node")
                .kernel("ForkGraphKernel")
                .jsonAttrs(R"json({"output_node_name": "inner_source", "output_node_port": "output1"})json")
                .binaryAttrsFromMap({{"fork_graph", forkGraphStr}, {"local_biz_name", naviGraphRunner.getBizName()}});
        auto downstream = builder.node("downstream").kernel("IdentityTestKernel");
        left.out("output1").to(downstream.in("input1"));
        downstream.out("output1").asGraphOutput("o");
    }
    ASSERT_TRUE(builder.ok());
    WaitClosure waitClosure;
    NaviFuncClosure closure([&waitClosure](NaviUserResultPtr naviUserResult) {
        autil::ScopeGuard guard([&waitClosure]() { waitClosure.notify(); });
        ASSERT_NE(nullptr, naviUserResult);
        auto naviResult = naviUserResult->getNaviResult();
        ASSERT_NE(nullptr, naviUserResult);
        ASSERT_EQ(EC_TIMEOUT, naviResult->getErrorCode()) << naviResult->getErrorMessage();
    });
    // NaviLoggerProvider logProvider("SCHEDULE1");
    int64_t timeoutMs = 1;
    naviGraphRunner.runLocalGraphAsync(graphDef.release(), {}, &closure, "", timeoutMs);
    waitClosure.wait();
    ASSERT_FALSE(HasFatalFailure());
}

} // namespace navi
