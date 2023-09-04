#include "unittest/unittest.h"
#include "navi/engine/Navi.h"
#include "navi/builder/GraphBuilder.h"
#include "navi/test_cluster/NaviGraphRunner.h"

using namespace std;
using namespace testing;

namespace navi {

class NaviTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void NaviTest::setUp() {
}

void NaviTest::tearDown() {
}

TEST_F(NaviTest, testRunGraphAfterStop) {
    NaviGraphRunner naviGraphRunner;
    ASSERT_TRUE(naviGraphRunner.init());

    naviGraphRunner._navi->stop();

    auto graphDef = std::make_unique<GraphDef>();
    GraphBuilder builder(graphDef.get());
    auto mainGraph = builder.newSubGraph(naviGraphRunner.getBizName());
    builder.subGraph(mainGraph);
    ASSERT_TRUE(builder.ok());

    // run local graph after navi stop will return nullptr
    auto naviUserResult = naviGraphRunner.runLocalGraph(graphDef.release(), {});
    ASSERT_EQ(nullptr, naviUserResult);
}

TEST_F(NaviTest, testWaitSnapshotUseCount) {
    NaviGraphRunner naviGraphRunner;
    ASSERT_TRUE(naviGraphRunner.init());

    auto snapshot = naviGraphRunner._navi->getSnapshot();
    ASSERT_NE(nullptr, snapshot);
    bool flag = false;
    auto th1 = autil::Thread::createThread([&]() {
        usleep(0.5 * 1000 * 1000);
        flag = true;
        snapshot.reset();
    });
    naviGraphRunner._navi->stop();
    ASSERT_TRUE(flag);
    
    th1->join();
}


}
