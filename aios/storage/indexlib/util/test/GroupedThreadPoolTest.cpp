#include "indexlib/util/GroupedThreadPool.h"

#include "autil/EnvUtil.h"
#include "indexlib/util/Exception.h"
#include "unittest/unittest.h"

namespace indexlib::util {

class GroupedThreadPoolTest : public TESTBASE
{
public:
    GroupedThreadPoolTest() = default;
    ~GroupedThreadPoolTest() = default;

public:
    void setUp() override {};
    void tearDown() override {};

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.util, GroupedThreadPoolTest);

TEST_F(GroupedThreadPoolTest, TestSimpleProcess)
{
    size_t THREAD_NUM = 4;
    size_t BATCH_NUM = 100;
    size_t GROUP_NUM = 300;
    size_t COUNT = 500;

    std::vector<int32_t> counts(GROUP_NUM, 0);

    GroupedThreadPool threadPool;
    threadPool.Legacy_Start("GroupedThreadPoolTest", THREAD_NUM, GROUP_NUM, BATCH_NUM);
    threadPool.Legacy_GetThreadPool()->start();
    for (int32_t batchId = 0; batchId < BATCH_NUM; ++batchId) {
        threadPool.StartNewBatch();
        for (int32_t groupId = 0; groupId < GROUP_NUM; ++groupId) {
            threadPool.PushTask("G" + std::to_string(groupId), [groupId, &COUNT, &counts]() {
                for (int32_t i = 0; i < COUNT; ++i) {
                    counts[groupId] += 1;
                }
            });
        }
        threadPool.AddBatchFinishHook([&counts, &COUNT, &GROUP_NUM]() {
            for (int32_t groupId = 0; groupId < GROUP_NUM; ++groupId) {
                if (counts[groupId] < COUNT) {
                    assert(false);
                }
            }
        });
    }
    threadPool.WaitFinish();
    for (int32_t groupId = 0; groupId < GROUP_NUM; ++groupId) {
        EXPECT_EQ(BATCH_NUM * COUNT, counts[groupId]) << "Group [" << groupId << "] ERROR";
    }
}

TEST_F(GroupedThreadPoolTest, TestLongTailFields)
{
    autil::EnvUtil::setEnv(GroupedThreadPool::LONG_TAIL_FIELD_ENV, "_OPERATION_LOG_,G1,G2");

    size_t THREAD_NUM = 4;
    size_t BATCH_NUM = 100;
    size_t GROUP_NUM = 300;
    size_t COUNT = 500;

    std::vector<int32_t> counts(GROUP_NUM, 0);

    GroupedThreadPool threadPool;
    threadPool.Legacy_Start("GroupedThreadPoolTest", THREAD_NUM, GROUP_NUM, BATCH_NUM);
    threadPool.Legacy_GetThreadPool()->start();
    for (int32_t batchId = 0; batchId < BATCH_NUM; ++batchId) {
        threadPool.StartNewBatch();
        for (int32_t groupId = 0; groupId < GROUP_NUM; ++groupId) {
            threadPool.PushTask("G" + std::to_string(groupId), [groupId, &COUNT, &counts]() {
                for (int32_t i = 0; i < COUNT; ++i) {
                    counts[groupId] += 1;
                }
            });
        }
        threadPool.AddBatchFinishHook([&counts, &COUNT, &GROUP_NUM]() {
            for (int32_t groupId = 0; groupId < GROUP_NUM; ++groupId) {
                if (counts[groupId] < COUNT) {
                    assert(false);
                }
            }
        });
    }
    threadPool.WaitFinish();
    for (int32_t groupId = 0; groupId < GROUP_NUM; ++groupId) {
        EXPECT_EQ(BATCH_NUM * COUNT, counts[groupId]) << "Group [" << groupId << "] ERROR";
    }
}

TEST_F(GroupedThreadPoolTest, TestTooManyGroups)
{
    size_t THREAD_NUM = 2;
    size_t GROUP_NUM = 100;
    size_t ONE_GROUP_ITEMS_NUM = 10;

    GroupedThreadPool threadPool;
    threadPool.Legacy_Start("GroupedThreadPoolTest", THREAD_NUM, GROUP_NUM, 10);
    threadPool.Legacy_GetThreadPool()->start();
    for (size_t groupId = 0; groupId < GROUP_NUM * 2; ++groupId) {
        for (size_t itemId = 0; itemId < ONE_GROUP_ITEMS_NUM; ++itemId) {
            threadPool.PushTask("group_" + std::to_string(groupId), []() { usleep(2 * 1000); });
        }
    }
    threadPool.WaitFinish();
}

TEST_F(GroupedThreadPoolTest, TestTooManyBatches)
{
    size_t THREAD_NUM = 1;
    size_t BATCH_NUM = 2;
    size_t GROUP_NUM = 4;
    size_t COUNT = 500;

    std::vector<int32_t> counts(GROUP_NUM, 0);

    GroupedThreadPool threadPool;
    threadPool.Legacy_Start("GroupedThreadPoolTest", THREAD_NUM, GROUP_NUM, BATCH_NUM);
    threadPool.Legacy_GetThreadPool()->start();
    for (int32_t i = 0; i < GROUP_NUM * 2; ++i) {
        int32_t groupId = i % GROUP_NUM;
        threadPool.StartNewBatch();
        threadPool.PushTask("G" + std::to_string(groupId), [groupId, &COUNT, &counts]() {
            for (int32_t i = 0; i < COUNT; ++i) {
                counts[groupId] += 1;
            }
            usleep(2000);
        });
        threadPool.AddBatchFinishHook([]() { usleep(2000); });
    }
    threadPool.WaitFinish();
    for (int32_t groupId = 0; groupId < GROUP_NUM; ++groupId) {
        EXPECT_EQ(BATCH_NUM * COUNT, counts[groupId]) << "Group [" << groupId << "] ERROR";
    }
}

TEST_F(GroupedThreadPoolTest, TestTooManyHooks)
{
    size_t THREAD_NUM = 2;
    size_t GROUP_NUM = 100;

    GroupedThreadPool threadPool;
    threadPool.Legacy_Start("GroupedThreadPoolTest", THREAD_NUM, GROUP_NUM, 10);
    threadPool.Legacy_GetThreadPool()->start();
    EXPECT_TRUE(threadPool.AddBatchFinishHook([]() {}));
    EXPECT_TRUE(threadPool.AddBatchFinishHook([]() {}));
    EXPECT_TRUE(threadPool.AddBatchFinishHook([]() {}));
    EXPECT_TRUE(threadPool.AddBatchFinishHook([]() {}));

    threadPool.PushTask("group_1", []() { usleep(1000 * 1000); });
    EXPECT_TRUE(threadPool.AddBatchFinishHook([]() {}));
    EXPECT_TRUE(threadPool.AddBatchFinishHook([]() {}));
    EXPECT_FALSE(threadPool.AddBatchFinishHook([]() {}));
    EXPECT_FALSE(threadPool.AddBatchFinishHook([]() {}));

    threadPool.WaitFinish();
}

TEST_F(GroupedThreadPoolTest, TestWaitCurrentBatchFinish)
{
    size_t THREAD_NUM = 2;
    size_t GROUP_NUM = 100;

    GroupedThreadPool threadPool;
    threadPool.Legacy_Start("GroupedThreadPoolTest", THREAD_NUM, GROUP_NUM, 10);
    threadPool.Legacy_GetThreadPool()->start();
    for (int i = 0; i < 100; ++i) {
        std::atomic<int32_t> count = 0;
        threadPool.StartNewBatch();
        for (int j = 0; j < i; ++j) {
            threadPool.PushTask("G" + std::to_string(j), [&count]() {
                count += 1;
                usleep(1);
            });
        }
        threadPool.WaitCurrentBatchWorkItemsFinish();

        EXPECT_EQ(i, count);
    }
    threadPool.WaitFinish();
}

TEST_F(GroupedThreadPoolTest, TestException)
{
    size_t THREAD_NUM = 2;
    size_t GROUP_NUM = 100;
    size_t BATCH_NUM = 100;

    GroupedThreadPool threadPool;
    threadPool.Legacy_Start("GroupedThreadPoolTest", THREAD_NUM, GROUP_NUM, 10);
    threadPool.Legacy_GetThreadPool()->start();
    std::atomic<int32_t> count = 0;
    for (int i = 0; i < 100; ++i) {
        threadPool.StartNewBatch();
        threadPool.PushTask("G", [&count]() {
            count += 1;
            usleep(10);
            INDEXLIB_THROW(util::RuntimeException, "trigger exception");
        });
    }
    threadPool.WaitFinish();
    EXPECT_EQ(BATCH_NUM, count);
}

TEST_F(GroupedThreadPoolTest, VerifyStopHook)
{
    std::unique_ptr<autil::ThreadPool> threadPool;
    uint16_t parallelNum = 4;
    std::atomic<uint16_t> stopedSingleProducerCount = 0;
    threadPool = std::make_unique<autil::ThreadPool>(parallelNum, parallelNum);
    for (size_t parallelIdx = 0; parallelIdx < parallelNum; ++parallelIdx) {
        threadPool->pushTask([parallelIdx, &stopedSingleProducerCount] {
            int counter = 0;
            while (true) {
                int ran = rand() % 10;
                if (ran == 0) {
                    counter++;
                }
                if (counter == 10) {
                    AUTIL_LOG(INFO, "parllelIdx[%ld] swift meet eof, exit", parallelIdx);
                    stopedSingleProducerCount++;
                    return;
                } else {
                    usleep(100);
                }
            }
        });
    }
    // threadPool->setThreadStopHook([this, &stopedSingleProducerCount]() {
    //     ++stopedSingleProducerCount;
    //     AUTIL_LOG(INFO, "swift meet eof, exit");
    // });
    threadPool->start();
    while (true) {
        if (stopedSingleProducerCount.load() == parallelNum) {
            break;
        } else {
            usleep(1000);
        }
    }
    threadPool->stop(autil::ThreadPool::STOP_AND_CLEAR_QUEUE_IGNORE_EXCEPTION);
}

} // namespace indexlib::util
