#include "sql/common/GenericWaiter.h"

#include <atomic>
#include <ext/alloc_traits.h>
#include <iosfwd>
#include <limits>
#include <memory>
#include <queue>
#include <stdint.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "autil/Lock.h"
#include "autil/TimeUtility.h"
#include "autil/result/ForwardList.h"
#include "autil/result/Result.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;
using namespace autil::result;

namespace sql {

class WatermarkWaiter;

struct CallbackParam {};

struct WatermarkWaiterPack {
    using ClassT = WatermarkWaiter;
    using StatusT = int64_t;
    using CallbackParamT = CallbackParam;
};

class WatermarkWaiter : public GenericWaiter<WatermarkWaiterPack> {
public:
    ~WatermarkWaiter() override {
        stop();
    }

public:
    std::string getDesc() const override {
        return "";
    }

public:
    int64_t getCurrentStatusImpl() const {
        return _watermark;
    }
    int64_t transToKeyImpl(const int64_t &status) const {
        return status;
    }
    CallbackParam transToCallbackParamImpl(const int64_t &status) const {
        return {};
    };

private:
    int64_t _watermark = 0;
};

using CallbackItem = WatermarkWaiter::CallbackItem;

class GenericWaiterTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void GenericWaiterTest::setUp() {}

void GenericWaiterTest::tearDown() {}

TEST_F(GenericWaiterTest, testInit) {
    WatermarkWaiter waiter;
    ASSERT_TRUE(waiter.init());
    usleep(1 * 1000 * 1000);
}

TEST_F(GenericWaiterTest, testWait_DirectReturn) {
    Notifier notifier;
    WatermarkWaiter waiter;
    waiter._watermark = 100;
    waiter.wait(
        99,
        [&](Result<CallbackParam> result) {
            EXPECT_TRUE(result.is_ok());
            notifier.notifyExit();
        },
        std::numeric_limits<int64_t>::max());
    ASSERT_TRUE(waiter._targetTsQueue.empty());
    ASSERT_TRUE(waiter._timeoutQueue.empty());
    ASSERT_EQ(0, waiter._pendingItemCount);
    notifier.waitNotification();
}

TEST_F(GenericWaiterTest, testWait_Success) {
    Notifier notifier1, notifier2, notifier3;
    WatermarkWaiter waiter;
    waiter.wait(
        3,
        [&](Result<CallbackParam> result) {
            EXPECT_TRUE(result.is_err());
            notifier1.notifyExit();
        },
        10000 * 1000);
    waiter.wait(
        2,
        [&](Result<CallbackParam> result) {
            EXPECT_TRUE(result.is_err());
            notifier2.notifyExit();
        },
        1000 * 1000);
    waiter.wait(
        1,
        [&](Result<CallbackParam> result) {
            EXPECT_TRUE(result.is_err());
            notifier3.notifyExit();
        },
        100 * 1000);

    ASSERT_EQ(3, waiter._targetTsQueue.size());
    ASSERT_EQ(3, waiter._timeoutQueue.size());
    ASSERT_EQ(3, waiter._pendingItemCount);
    {
        // check checkpoint queue pop order
        auto queue = waiter._targetTsQueue;
        ASSERT_FALSE(queue.empty());
        ASSERT_EQ(1, queue.top().targetTs);
        ASSERT_EQ(100 * 1000, queue.top().payload->timeoutInUs);
        ASSERT_TRUE(queue.top().payload->callback);
        queue.pop();

        ASSERT_FALSE(queue.empty());
        ASSERT_EQ(2, queue.top().targetTs);
        ASSERT_EQ(1000 * 1000, queue.top().payload->timeoutInUs);
        ASSERT_TRUE(queue.top().payload->callback);
        queue.pop();

        ASSERT_FALSE(queue.empty());
        ASSERT_EQ(3, queue.top().targetTs);
        ASSERT_EQ(10000 * 1000, queue.top().payload->timeoutInUs);
        ASSERT_TRUE(queue.top().payload->callback);
        queue.pop();

        ASSERT_TRUE(queue.empty());
    }

    {
        // check timeout queue pop order
        auto queue = waiter._timeoutQueue;
        ASSERT_FALSE(queue.empty());
        ASSERT_EQ(1, queue.top().targetTs);
        ASSERT_EQ(100 * 1000, queue.top().payload->timeoutInUs);
        ASSERT_GE(TimeUtility::currentTime() + 100 * 1000, queue.top().expireTimeInUs);
        ASSERT_TRUE(queue.top().payload->callback);
        queue.pop();

        ASSERT_FALSE(queue.empty());
        ASSERT_EQ(2, queue.top().targetTs);
        ASSERT_EQ(1000 * 1000, queue.top().payload->timeoutInUs);
        ASSERT_GE(TimeUtility::currentTime() + 1000 * 1000, queue.top().expireTimeInUs);
        ASSERT_TRUE(queue.top().payload->callback);
        queue.pop();

        ASSERT_FALSE(queue.empty());
        ASSERT_EQ(3, queue.top().targetTs);
        ASSERT_EQ(10000 * 1000, queue.top().payload->timeoutInUs);
        ASSERT_GE(TimeUtility::currentTime() + 10000 * 1000, queue.top().expireTimeInUs);
        ASSERT_TRUE(queue.top().payload->callback);
        queue.pop();

        ASSERT_TRUE(queue.empty());
    }

    waiter.stop();
    notifier1.waitNotification();
    notifier2.waitNotification();
    notifier3.waitNotification();
}

TEST_F(GenericWaiterTest, testCheckCallback_WatermarkReady) {
    Notifier notifier1, notifier2, notifier3;
    WatermarkWaiter waiter;
    size_t callbackOrder = 0;
    {
        CallbackItem item;
        item.payload->callback = [&](Result<CallbackParam> result) {
            EXPECT_TRUE(result.is_ok());
            notifier1.notifyExit();
            EXPECT_EQ(3, ++callbackOrder);
        };
        item.targetTs = 103;
        waiter._targetTsQueue.emplace(item);
    }
    {
        CallbackItem item;
        item.payload->callback = [&](Result<CallbackParam> result) {
            EXPECT_TRUE(result.is_ok());
            notifier2.notifyExit();
            EXPECT_EQ(2, ++callbackOrder);
        };
        item.targetTs = 102;
        waiter._targetTsQueue.emplace(item);
    }
    {
        CallbackItem item;
        item.payload->callback = [&](Result<CallbackParam> result) {
            EXPECT_TRUE(result.is_ok());
            notifier3.notifyExit();
            EXPECT_EQ(1, ++callbackOrder);
        };
        item.targetTs = 101;
        waiter._targetTsQueue.emplace(item);
    }
    waiter._pendingItemCount = waiter._targetTsQueue.size();

    waiter._watermark = 101;
    waiter.checkCallback();
    ASSERT_EQ(3, waiter._targetTsQueue.size());
    ASSERT_EQ(3, waiter._pendingItemCount);
    waiter._watermark = 102;
    waiter.checkCallback();
    ASSERT_EQ(2, waiter._targetTsQueue.size());
    ASSERT_EQ(2, waiter._pendingItemCount);
    waiter._watermark = 103;
    waiter.checkCallback();
    ASSERT_EQ(1, waiter._targetTsQueue.size());
    ASSERT_EQ(1, waiter._pendingItemCount);
    waiter._watermark = 104;
    waiter.checkCallback();
    ASSERT_EQ(0, waiter._targetTsQueue.size());
    ASSERT_EQ(0, waiter._pendingItemCount);

    notifier1.waitNotification();
    notifier2.waitNotification();
    notifier3.waitNotification();
}

TEST_F(GenericWaiterTest, testCheckCallback_timeoutReady) {
    Notifier notifier1, notifier2, notifier3;
    WatermarkWaiter waiter;
    size_t callbackOrder = 0;
    {
        CallbackItem item;
        item.payload->callback = [&](Result<CallbackParam> result) {
            EXPECT_TRUE(result.is_err());
            notifier1.notifyExit();
            EXPECT_EQ(3, ++callbackOrder);
        };
        item.expireTimeInUs = std::numeric_limits<int64_t>::max();
        waiter._timeoutQueue.emplace(item);
    }
    {
        CallbackItem item;
        item.payload->callback = [&](Result<CallbackParam> result) {
            EXPECT_TRUE(result.is_err());
            notifier2.notifyExit();
            EXPECT_EQ(2, ++callbackOrder);
        };
        item.expireTimeInUs = 2;
        waiter._timeoutQueue.emplace(item);
    }
    {
        CallbackItem item;
        item.payload->callback = [&](Result<CallbackParam> result) {
            EXPECT_TRUE(result.is_err());
            notifier3.notifyExit();
            EXPECT_EQ(1, ++callbackOrder);
        };
        item.expireTimeInUs = 1;
        waiter._timeoutQueue.emplace(item);
    }
    waiter._pendingItemCount = waiter._timeoutQueue.size();
    waiter.checkCallback();
    ASSERT_EQ(1, waiter._timeoutQueue.size());
    ASSERT_EQ(1, waiter._pendingItemCount);

    waiter.checkCallback();
    ASSERT_EQ(1, waiter._timeoutQueue.size());
    ASSERT_EQ(1, waiter._pendingItemCount);

    (const_cast<CallbackItem &>(waiter._timeoutQueue.top())).expireTimeInUs = 0;
    waiter.checkCallback();
    ASSERT_EQ(0, waiter._timeoutQueue.size());
    ASSERT_EQ(0, waiter._pendingItemCount);

    notifier1.waitNotification();
    notifier2.waitNotification();
    notifier3.waitNotification();
}

TEST_F(GenericWaiterTest, testAll) {
    Notifier notifier;
    WatermarkWaiter waiter;
    ASSERT_TRUE(waiter.init());
    waiter.wait(
        1,
        [&](Result<CallbackParam> result) {
            EXPECT_TRUE(result.is_ok());
            notifier.notifyExit();
        },
        std::numeric_limits<int64_t>::max());
    usleep(1000 * 1000); // 1s
    waiter._watermark = 2;
    notifier.waitNotification();
    ASSERT_EQ(0, waiter.getPendingItemCount());
}

} // namespace sql
