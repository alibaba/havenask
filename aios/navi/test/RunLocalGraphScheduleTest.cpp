#include "autil/Thread.h"
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
#include "unittest/TestEnvScope.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;

namespace navi {

class RunLocalGraphScheduleTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void RunLocalGraphScheduleTest::setUp() {}

void RunLocalGraphScheduleTest::tearDown() {}

TEST_F(RunLocalGraphScheduleTest, testOneProcessingMaxAndOneScheduleQueue) {
    NaviGraphRunner naviGraphRunner;
    ASSERT_TRUE(naviGraphRunner.init());

    auto graphDef1 = std::make_unique<GraphDef>();
    {
        GraphBuilder builder(graphDef1.get());
        builder.newSubGraph(naviGraphRunner.getBizName());
        builder.node("node1")
            .kernel("SourceKernel")
            .jsonAttrs(R"json({"sleep_ms" : 3000})json")
            .out("output1")
            .asGraphOutput("o");
        ASSERT_TRUE(builder.ok());
    }

    auto graphDef2 = std::make_unique<GraphDef>();
    {
        GraphBuilder builder(graphDef2.get());
        builder.newSubGraph(naviGraphRunner.getBizName());
        builder.node("node1")
            .kernel("SourceKernel")
            .jsonAttrs(R"json({"sleep_ms" : 10})json")
            .out("output1")
            .asGraphOutput("o");
        ASSERT_TRUE(builder.ok());
    }
    auto graphDef3 = std::make_unique<GraphDef>();
    {
        GraphBuilder builder(graphDef3.get());
        builder.newSubGraph(naviGraphRunner.getBizName());
        builder.node("node1")
            .kernel("SourceKernel")
            .jsonAttrs(R"json({"sleep_ms" : 10})json")
            .out("output1")
            .asGraphOutput("o");
        ASSERT_TRUE(builder.ok());
    }

    auto naviUserResult = naviGraphRunner.runLocalGraph(graphDef1.release(), {}, "slow_slow_queue");
    auto naviUserResult2 = naviGraphRunner.runLocalGraph(graphDef2.release(), {}, "slow_slow_queue");
    auto naviUserResult3 = naviGraphRunner.runLocalGraph(graphDef3.release(), {}, "slow_slow_queue");
    ASSERT_TRUE(nullptr == naviUserResult3);
    std::atomic<bool> firstDone = false;
    while (true) {
        NaviUserData data;
        bool eof = false;
        naviUserResult->nextData(data, eof);
        firstDone = true;
        if (eof) {
            break;
        }
    }
    while (true) {
        NaviUserData data;
        bool eof = false;
        naviUserResult2->nextData(data, eof);
        firstDone = true;
        if (eof) {
            break;
        }
    }
}

TEST_F(RunLocalGraphScheduleTest, testOneProcessingMaxMultiScheduleQueue) {
    NaviGraphRunner naviGraphRunner;
    ASSERT_TRUE(naviGraphRunner.init());

    auto graphDef1 = std::make_unique<GraphDef>();
    {
        GraphBuilder builder(graphDef1.get());
        builder.newSubGraph(naviGraphRunner.getBizName());
        builder.node("node1")
            .kernel("SourceKernel")
            .jsonAttrs(R"json({"sleep_ms" : 1000})json")
            .out("output1")
            .asGraphOutput("o");
        ASSERT_TRUE(builder.ok());
    }
    auto graphDef2 = std::make_unique<GraphDef>();
    {
        GraphBuilder builder(graphDef2.get());
        builder.newSubGraph(naviGraphRunner.getBizName());
        builder.node("node1")
            .kernel("SourceKernel")
            .jsonAttrs(R"json({"sleep_ms" : 10})json")
            .out("output1")
            .asGraphOutput("o");
        ASSERT_TRUE(builder.ok());
    }

    auto naviUserResult = naviGraphRunner.runLocalGraph(graphDef1.release(), {}, "slow_queue");
    auto naviUserResult2 = naviGraphRunner.runLocalGraph(graphDef2.release(), {}, "slow_queue");

    std::atomic<bool> firstDone = false;
    auto th1 = autil::Thread::createThread([&]() {
        while (true) {
            NaviUserData data;
            bool eof = false;
            naviUserResult->nextData(data, eof);
            firstDone = true;
            if (eof) {
                break;
            }
        }
    });

    auto th2 = autil::Thread::createThread([&]() {
        while (true) {
            NaviUserData data;
            bool eof = false;
            naviUserResult2->nextData(data, eof);
            ASSERT_TRUE(naviUserResult->_terminated);
            if (eof) {
                break;
            }
        }
    });
    th1->join();
    th2->join();
}

} // namespace navi
