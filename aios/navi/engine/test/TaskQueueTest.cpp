#include "navi/engine/TaskQueue.h"

#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace navi {

class MockTaskQueueScheduleItemBase : public TaskQueueScheduleItemBase {
public:
    MockTaskQueueScheduleItemBase() {}

public:
    MOCK_METHOD0(process, void());
    MOCK_METHOD0(destroy, void());
};

class MockTaskQueue : public TaskQueue {
public:
    MockTaskQueue() {}

private:
    MOCK_METHOD0(schedule, void());
};

class TaskQueueTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void TaskQueueTest::setUp() {}

void TaskQueueTest::tearDown() {}


TEST_F(TaskQueueTest, testPush_testDrop) {
    StrictMock<MockTaskQueue> taskQueue;
    taskQueue._testMode = TM_NONE;
    taskQueue._processingMax = 0;
    taskQueue._scheduleQueueMax = 0;
    StrictMock<MockTaskQueueScheduleItemBase> item;
    EXPECT_CALL(item, destroy()).WillOnce(Return());
    ASSERT_FALSE(taskQueue.push(&item));
    ASSERT_EQ(0, taskQueue.getProcessingCount());
    ASSERT_EQ(0, taskQueue._scheduleQueue.Size());
}

TEST_F(TaskQueueTest, testPush_testScheduled) {
    StrictMock<MockTaskQueue> taskQueue;
    taskQueue._testMode = TM_NONE;
    taskQueue._processingMax = 0;
    taskQueue._scheduleQueueMax = 1;
    EXPECT_CALL(taskQueue, schedule()).WillOnce(Return());
    StrictMock<MockTaskQueueScheduleItemBase> item;
    ASSERT_TRUE(taskQueue.push(&item));
    ASSERT_EQ(0, taskQueue.getProcessingCount());
    ASSERT_EQ(1, taskQueue._scheduleQueue.Size());
}

TEST_F(TaskQueueTest, testSchedule_testProcessingCountMoreThanMax) {
    TaskQueue taskQueue;
    taskQueue._processingMax = 0;
    StrictMock<MockTaskQueueScheduleItemBase> item;
    taskQueue._scheduleQueue.Push(&item);
    atomic_set(&taskQueue._processingCount, 1);
    taskQueue.schedule();
    ASSERT_EQ(1, taskQueue.getProcessingCount());
    ASSERT_EQ(1, taskQueue._scheduleQueue.Size());
}

TEST_F(TaskQueueTest, testSchedule_testProcessSuccess) {
    TaskQueue taskQueue;
    ConcurrencyConfig config;
    config.threadNum = 1;
    taskQueue.init("test", config, TM_KERNEL_TEST);
    taskQueue._processingMax = 1;

    StrictMock<MockTaskQueueScheduleItemBase> item;
    taskQueue._scheduleQueue.Push(&item);
    EXPECT_CALL(item, process()).WillOnce(Return());
    EXPECT_CALL(item, destroy()).WillOnce(Return());
    taskQueue.schedule();
    ASSERT_EQ(1, taskQueue.getProcessingCount());
    ASSERT_EQ(0, taskQueue._scheduleQueue.Size());
}

TEST_F(TaskQueueTest, testScheduleNext) {
    StrictMock<MockTaskQueue> taskQueue;
    atomic_set(&taskQueue._processingCount, 1);
    taskQueue._processingMax = 1;
    EXPECT_CALL(taskQueue, schedule()).WillOnce(Return());
    taskQueue.scheduleNext();
    ASSERT_EQ(0, taskQueue.getProcessingCount());
}

} // namespace navi
