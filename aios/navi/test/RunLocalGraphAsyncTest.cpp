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

class RunLocalGraphAsyncTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void RunLocalGraphAsyncTest::setUp() {}

void RunLocalGraphAsyncTest::tearDown() {}

TEST_F(RunLocalGraphAsyncTest, testRunLocalGraph_AbortCompute) {
    NaviGraphRunner naviGraphRunner;
    ASSERT_TRUE(naviGraphRunner.init());
    auto graphDef = std::make_unique<GraphDef>();
    GraphBuilder builder(graphDef.get());
    builder.newSubGraph(naviGraphRunner.getBizName());
    auto source = builder.node("source").kernel("SourceKernel");
    auto abort = builder.node("abort").kernel("AbortKernel");
    abort.in("input1").from(source.out("output1"));
    abort.out("output1").asGraphOutput("o");
    ASSERT_TRUE(builder.ok());
    WaitClosure waitClosure;
    NaviFuncClosure closure([&waitClosure](NaviUserResultPtr naviUserResult) {
        autil::ScopeGuard guard([&waitClosure]() { waitClosure.notify(); });
        ASSERT_NE(nullptr, naviUserResult);
        auto naviResult = naviUserResult->getNaviResult();
        ASSERT_EQ(EC_ABORT, naviResult->ec) << naviResult->errorEvent.message;
    });
    naviGraphRunner.runLocalGraphAsync(graphDef.release(), {}, &closure);
    waitClosure.wait();
}

TEST_F(RunLocalGraphAsyncTest, testRunLocalGraph_InitAbortCompute) {
    NaviGraphRunner naviGraphRunner;
    ASSERT_TRUE(naviGraphRunner.init());
    auto graphDef = std::make_unique<GraphDef>();
    GraphBuilder builder(graphDef.get());
    builder.newSubGraph(naviGraphRunner.getBizName());
    builder.node("source").kernel("InitAbortKernel").out("output1").asGraphOutput("o");
    ASSERT_TRUE(builder.ok());
    WaitClosure waitClosure;
    NaviFuncClosure closure([&waitClosure](NaviUserResultPtr naviUserResult) {
        autil::ScopeGuard guard([&waitClosure]() { waitClosure.notify(); });
        ASSERT_NE(nullptr, naviUserResult);
        auto naviResult = naviUserResult->getNaviResult();
        ASSERT_EQ(EC_ABORT, naviResult->ec) << naviResult->errorEvent.message;
    });
    naviGraphRunner.runLocalGraphAsync(graphDef.release(), {}, &closure);
    waitClosure.wait();
}

TEST_F(RunLocalGraphAsyncTest, testRunLocalGraph_MultipleOutput) {
    NaviGraphRunner naviGraphRunner;
    ASSERT_TRUE(naviGraphRunner.init());
    auto graphDef = std::make_unique<GraphDef>();
    GraphBuilder builder(graphDef.get());
    builder.newSubGraph(naviGraphRunner.getBizName());
    builder.node("source")
        .kernel("SourceKernel")
        .jsonAttrs(R"json({"times" : 10})json")
        .out("output1")
        .asGraphOutput("o");
    ASSERT_TRUE(builder.ok());
    WaitClosure waitClosure;
    NaviFuncClosure closure([&waitClosure](NaviUserResultPtr naviUserResult) {
        autil::ScopeGuard guard([&waitClosure]() { waitClosure.notify(); });
        ASSERT_NE(nullptr, naviUserResult);
        auto naviResult = naviUserResult->getNaviResult();
        ASSERT_NE(nullptr, naviUserResult);
        ASSERT_EQ(EC_NONE, naviResult->ec) << naviResult->errorEvent.message;

        for (size_t i = 0; i < 10; ++i) {
            NaviUserData data;
            bool eof = false;
            ASSERT_TRUE(naviUserResult->nextData(data, eof)) << "times: " << i;
            auto *helloData = dynamic_cast<HelloData *>(data.data.get());
            ASSERT_NE(nullptr, helloData);
            if (i == 9) {
                ASSERT_TRUE(eof);
            } else {
                ASSERT_FALSE(eof);
            }
        }
    });
    naviGraphRunner.runLocalGraphAsync(graphDef.release(), {}, &closure);
    waitClosure.wait();
}

TEST_F(RunLocalGraphAsyncTest, testRunLocalGraph_MultipleSourceKernel) {
    NaviGraphRunner naviGraphRunner;
    ASSERT_TRUE(naviGraphRunner.init());
    auto graphDef = std::make_unique<GraphDef>();
    GraphBuilder builder(graphDef.get());
    builder.newSubGraph(naviGraphRunner.getBizName());
    for (size_t i = 0; i < 10; ++i) {
        builder.node(std::string("source") + std::to_string(i))
            .kernel("SourceKernel")
            .jsonAttrs(R"json({"times" : 1})json")
            .out("output1")
            .asGraphOutput(std::string("o") + std::to_string(i));
    }
    ASSERT_TRUE(builder.ok());
    WaitClosure waitClosure;
    NaviFuncClosure closure([&waitClosure](NaviUserResultPtr naviUserResult) {
        autil::ScopeGuard guard([&waitClosure]() { waitClosure.notify(); });
        ASSERT_NE(nullptr, naviUserResult);
        auto naviResult = naviUserResult->getNaviResult();
        ASSERT_NE(nullptr, naviUserResult);
        ASSERT_EQ(EC_NONE, naviResult->ec) << naviResult->errorEvent.message;

        for (size_t i = 0; i < 10; ++i) {
            NaviUserData data;
            bool eof = false;
            ASSERT_TRUE(naviUserResult->nextData(data, eof)) << "times: " << i;
            auto *helloData = dynamic_cast<HelloData *>(data.data.get());
            ASSERT_NE(nullptr, helloData) << i;
            if (i == 9) {
                ASSERT_TRUE(eof) << i;
            } else {
                ASSERT_FALSE(eof) << i;
            }
        }
    });
    naviGraphRunner.runLocalGraphAsync(graphDef.release(), {}, &closure);
    waitClosure.wait();
}


} // namespace navi
