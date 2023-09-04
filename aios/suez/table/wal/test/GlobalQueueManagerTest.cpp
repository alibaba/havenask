#include "suez/table/wal/GlobalQueueManager.h"

#include <unittest/unittest.h>

using namespace std;
using namespace testing;

namespace suez {

class GlobalQueueManagerTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void GlobalQueueManagerTest::setUp() {}

void GlobalQueueManagerTest::tearDown() {}

TEST_F(GlobalQueueManagerTest, testCreateQueue) {
    GlobalQueueManager manager;
    auto queue1 = manager.createQueue("test1");
    EXPECT_TRUE(nullptr != queue1);
    EXPECT_EQ((size_t)1, manager._docQueueMap.size());

    auto queue2 = manager.createQueue("test1");
    EXPECT_TRUE(queue1 == queue2);
    EXPECT_EQ((size_t)1, manager._docQueueMap.size());
}

TEST_F(GlobalQueueManagerTest, testReleaseQueue) {
    GlobalQueueManager manager;
    auto queue1 = manager.createQueue("test1");
    EXPECT_TRUE(nullptr != queue1);
    EXPECT_EQ((size_t)1, manager._docQueueMap.size());

    manager.releaseQueue("test1");
    EXPECT_TRUE(manager._docQueueMap.empty());

    manager.releaseQueue("test2");
    EXPECT_TRUE(manager._docQueueMap.empty());
    EXPECT_TRUE(manager._docQueueMap.empty());
}

} // namespace suez
