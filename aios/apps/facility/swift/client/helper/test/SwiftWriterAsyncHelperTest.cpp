#include "swift/client/helper/SwiftWriterAsyncHelper.h"

#include <cstdint>
#include <ext/alloc_traits.h>
#include <functional>
#include <gmock/gmock-function-mocker.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-matchers.h>
#include <iosfwd>
#include <limits>
#include <memory>
#include <queue>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/Thread.h"
#include "autil/TimeUtility.h"
#include "autil/result/ForwardList.h"
#include "autil/result/Result.h"
#include "swift/client/MessageInfo.h"
#include "swift/client/Notifier.h"
#include "swift/client/SwiftWriter.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/testlib/MockSwiftWriter.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;
using namespace autil::result;
using namespace swift::testlib;
using namespace swift::protocol;

namespace swift {
namespace client {

class InnerMockSwiftWriterAsyncHelper : public swift::client::SwiftWriterAsyncHelper {
private:
    MOCK_CONST_METHOD0(getCommittedCkptId, int64_t(void));
    MOCK_CONST_METHOD0(getCkptIdLimit, int64_t(void));
    MOCK_METHOD2(stealRange, void(std::vector<int64_t> &timestamps, int64_t ckptId));
};

class SwiftWriterAsyncHelperTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void SwiftWriterAsyncHelperTest::setUp() {}

void SwiftWriterAsyncHelperTest::tearDown() {}

TEST_F(SwiftWriterAsyncHelperTest, testInit) {
    auto writerPtr = make_unique<MockSwiftWriter>();
    auto *writer = writerPtr.get();
    EXPECT_CALL(*writer, getCommittedCheckpointId()).WillOnce(Return(-1)).WillRepeatedly(Return(0));
    EXPECT_CALL(*writer, setCommittedCallBack(_)).WillOnce(Return());
    SwiftWriterAsyncHelper helper;
    ASSERT_TRUE(helper.init(std::move(writerPtr), ""));
    usleep(1 * 1000 * 1000);
}

TEST_F(SwiftWriterAsyncHelperTest, testWrite_ZeroMsg) {
    auto writerPtr = make_unique<MockSwiftWriter>();
    InnerMockSwiftWriterAsyncHelper helper;
    helper._writer = std::move(writerPtr);

    Notifier notifier;
    notifier.setNeedNotify();
    helper.write(
        nullptr,
        0,
        [&](Result<SwiftWriteCallbackParam> result) {
            EXPECT_TRUE(result.is_ok());
            notifier.notify();
        },
        std::numeric_limits<int64_t>::max());
    EXPECT_EQ(0, helper._checkpointIdAllocator);
    ASSERT_TRUE(helper._checkpointIdQueue.empty());
    ASSERT_TRUE(helper._timeoutQueue.empty());
    ASSERT_EQ(0, helper._pendingItemCount);
    notifier.wait(-1);
}

TEST_F(SwiftWriterAsyncHelperTest, testWrite_BufferFullError) {
    auto writerPtr = make_unique<MockSwiftWriter>();
    InnerMockSwiftWriterAsyncHelper helper;
    helper._writer = std::move(writerPtr);

    EXPECT_CALL(helper, getCkptIdLimit()).WillOnce(Return(0));

    MessageInfo msgInfo;
    Notifier notifier;
    notifier.setNeedNotify();
    helper.write(
        &msgInfo,
        1,
        [&](Result<SwiftWriteCallbackParam> result) {
            EXPECT_TRUE(result.is_err());
            notifier.notify();
        },
        std::numeric_limits<int64_t>::max());
    EXPECT_EQ(0, helper._checkpointIdAllocator);
    ASSERT_TRUE(helper._checkpointIdQueue.empty());
    ASSERT_TRUE(helper._timeoutQueue.empty());
    ASSERT_EQ(0, helper._pendingItemCount);
    notifier.wait(-1);
}

TEST_F(SwiftWriterAsyncHelperTest, testWrite_WriterError) {
    auto writerPtr = make_unique<MockSwiftWriter>();
    auto *writer = writerPtr.get();
    InnerMockSwiftWriterAsyncHelper helper;
    helper._writer = std::move(writerPtr);

    EXPECT_CALL(helper, getCkptIdLimit()).WillOnce(Return(std::numeric_limits<int64_t>::max()));
    EXPECT_CALL(*writer, write(_)).WillOnce(Return(ErrorCode::ERROR_CLIENT_RPC_CONNECT)).RetiresOnSaturation();

    MessageInfo msgInfo;
    Notifier notifier;
    notifier.setNeedNotify();
    helper.write(
        &msgInfo,
        1,
        [&](Result<SwiftWriteCallbackParam> result) {
            EXPECT_TRUE(result.is_err());
            notifier.notify();
        },
        std::numeric_limits<int64_t>::max());
    EXPECT_EQ(0, helper._checkpointIdAllocator);
    ASSERT_TRUE(helper._checkpointIdQueue.empty());
    ASSERT_TRUE(helper._timeoutQueue.empty());
    ASSERT_EQ(0, helper._pendingItemCount);
    notifier.wait(-1);
}

TEST_F(SwiftWriterAsyncHelperTest, testWrite_Success) {
    auto writerPtr = make_unique<MockSwiftWriter>();
    auto *writer = writerPtr.get();
    InnerMockSwiftWriterAsyncHelper helper;
    helper._writer = std::move(writerPtr);
    helper._checkpointIdAllocator = 0;

    EXPECT_CALL(helper, getCkptIdLimit()).Times(3).WillRepeatedly(Return(std::numeric_limits<int64_t>::max()));
    EXPECT_CALL(*writer, write(_)).Times(3).WillRepeatedly(Return(ErrorCode::ERROR_NONE));

    MessageInfo msgInfo1, msgInfo2, msgInfo3;
    Notifier notifier1, notifier2, notifier3;
    notifier1.setNeedNotify();
    notifier2.setNeedNotify();
    notifier3.setNeedNotify();
    helper.write(
        &msgInfo1,
        1,
        [&](Result<SwiftWriteCallbackParam> result) {
            EXPECT_TRUE(result.is_err());
            notifier1.notify();
        },
        10000 * 1000);
    ASSERT_EQ(0, msgInfo1.checkpointId);
    EXPECT_EQ(1, helper._checkpointIdAllocator);
    helper.write(
        &msgInfo2,
        1,
        [&](Result<SwiftWriteCallbackParam> result) {
            EXPECT_TRUE(result.is_err());
            notifier2.notify();
        },
        1000 * 1000);
    ASSERT_EQ(1, msgInfo2.checkpointId);
    EXPECT_EQ(2, helper._checkpointIdAllocator);
    helper.write(
        &msgInfo3,
        1,
        [&](Result<SwiftWriteCallbackParam> result) {
            EXPECT_TRUE(result.is_err());
            notifier3.notify();
        },
        100 * 1000);
    ASSERT_EQ(2, msgInfo3.checkpointId);
    EXPECT_EQ(3, helper._checkpointIdAllocator);

    ASSERT_EQ(3, helper._checkpointIdQueue.size());
    ASSERT_EQ(3, helper._timeoutQueue.size());
    ASSERT_EQ(3, helper._pendingItemCount);
    {
        // check checkpoint queue pop order
        auto queue = helper._checkpointIdQueue;
        ASSERT_FALSE(queue.empty());
        ASSERT_EQ(0, queue.front().checkpointId);
        ASSERT_EQ(10000 * 1000, queue.front().payload->timeoutInUs);
        ASSERT_TRUE(queue.front().payload->callback);
        queue.pop_front();

        ASSERT_FALSE(queue.empty());
        ASSERT_EQ(1, queue.front().checkpointId);
        ASSERT_EQ(1000 * 1000, queue.front().payload->timeoutInUs);
        ASSERT_TRUE(queue.front().payload->callback);
        queue.pop_front();

        ASSERT_FALSE(queue.empty());
        ASSERT_EQ(2, queue.front().checkpointId);
        ASSERT_EQ(100 * 1000, queue.front().payload->timeoutInUs);
        ASSERT_TRUE(queue.front().payload->callback);
        queue.pop_front();

        ASSERT_TRUE(queue.empty());
    }

    {
        // check timeout queue pop order
        auto queue = helper._timeoutQueue;
        ASSERT_FALSE(queue.empty());
        ASSERT_EQ(2, queue.top().checkpointId);
        ASSERT_EQ(100 * 1000, queue.top().payload->timeoutInUs);
        ASSERT_GE(TimeUtility::currentTime() + 100 * 1000, queue.top().expireTimeInUs);
        ASSERT_TRUE(queue.top().payload->callback);
        queue.pop();

        ASSERT_FALSE(queue.empty());
        ASSERT_EQ(1, queue.top().checkpointId);
        ASSERT_EQ(1000 * 1000, queue.top().payload->timeoutInUs);
        ASSERT_GE(TimeUtility::currentTime() + 1000 * 1000, queue.top().expireTimeInUs);
        ASSERT_TRUE(queue.top().payload->callback);
        queue.pop();

        ASSERT_FALSE(queue.empty());
        ASSERT_EQ(0, queue.top().checkpointId);
        ASSERT_EQ(10000 * 1000, queue.top().payload->timeoutInUs);
        ASSERT_GE(TimeUtility::currentTime() + 10000 * 1000, queue.top().expireTimeInUs);
        ASSERT_TRUE(queue.top().payload->callback);
        queue.pop();

        ASSERT_TRUE(queue.empty());
    }

    helper.stop();
    notifier1.wait(-1);
    notifier2.wait(-1);
    notifier3.wait(-1);
}

TEST_F(SwiftWriterAsyncHelperTest, testWrite_Success_NoTimeout) {
    auto writerPtr = make_unique<MockSwiftWriter>();
    auto *writer = writerPtr.get();
    InnerMockSwiftWriterAsyncHelper helper;
    helper._writer = std::move(writerPtr);
    helper._checkpointIdAllocator = 0;

    EXPECT_CALL(helper, getCkptIdLimit()).WillOnce(Return(std::numeric_limits<int64_t>::max()));
    EXPECT_CALL(*writer, write(_)).WillOnce(Return(ErrorCode::ERROR_NONE)).RetiresOnSaturation();

    MessageInfo msgInfo;
    Notifier notifier;
    notifier.setNeedNotify();
    helper.write(
        &msgInfo,
        1,
        [&](Result<SwiftWriteCallbackParam> result) {
            EXPECT_TRUE(result.is_err());
            notifier.notify();
        },
        std::numeric_limits<int64_t>::max());
    EXPECT_EQ(1, helper._checkpointIdAllocator);
    ASSERT_EQ(1, helper._checkpointIdQueue.size());
    ASSERT_TRUE(helper._timeoutQueue.empty());
    ASSERT_EQ(1, helper._pendingItemCount);
    auto &item = helper._checkpointIdQueue.front();
    ASSERT_TRUE(item.payload->callback);
    ASSERT_EQ(0, item.checkpointId);
    ASSERT_GE(std::numeric_limits<int64_t>::max(), item.expireTimeInUs);
    helper.stop();
    notifier.wait(-1);
}

TEST_F(SwiftWriterAsyncHelperTest, testCheckCallback_checkpointReady) {
    InnerMockSwiftWriterAsyncHelper helper;

    Notifier notifier1, notifier2, notifier3;
    notifier1.setNeedNotify();
    notifier2.setNeedNotify();
    notifier3.setNeedNotify();
    size_t callbackOrder = 0;
    {
        SwiftWriteCallbackItem item;
        item.payload->callback = [&](Result<SwiftWriteCallbackParam> result) {
            EXPECT_TRUE(result.is_ok());
            notifier3.notify();
            EXPECT_EQ(1, ++callbackOrder);
        };
        item.checkpointId = 101;
        item.payload->count = 1;
        helper._checkpointIdQueue.emplace_back(item);
    }
    {
        SwiftWriteCallbackItem item;
        item.payload->callback = [&](Result<SwiftWriteCallbackParam> result) {
            EXPECT_TRUE(result.is_ok());
            notifier2.notify();
            EXPECT_EQ(2, ++callbackOrder);
        };
        item.checkpointId = 102;
        item.payload->count = 1;
        helper._checkpointIdQueue.emplace_back(item);
    }
    {
        SwiftWriteCallbackItem item;
        item.payload->callback = [&](Result<SwiftWriteCallbackParam> result) {
            EXPECT_TRUE(result.is_ok());
            notifier1.notify();
            EXPECT_EQ(3, ++callbackOrder);
        };
        item.checkpointId = 103;
        item.payload->count = 1;
        helper._checkpointIdQueue.emplace_back(item);
    }
    helper._pendingItemCount = helper._checkpointIdQueue.size();

    EXPECT_CALL(helper, getCommittedCkptId())
        .WillOnce(Return(100))
        .WillOnce(Return(101))
        .WillOnce(Return(102))
        .WillOnce(Return(103));

    EXPECT_CALL(helper, stealRange(_, _))
        .Times(3)
        .WillRepeatedly(testing::Invoke([](std::vector<int64_t> &timestamp, int64_t ckptId) { timestamp.resize(1); }));

    helper.checkCallback();
    ASSERT_EQ(3, helper._checkpointIdQueue.size());
    ASSERT_EQ(3, helper._pendingItemCount);
    helper.checkCallback();
    ASSERT_EQ(2, helper._checkpointIdQueue.size());
    ASSERT_EQ(2, helper._pendingItemCount);
    helper.checkCallback();
    ASSERT_EQ(1, helper._checkpointIdQueue.size());
    ASSERT_EQ(1, helper._pendingItemCount);
    helper.checkCallback();
    ASSERT_EQ(0, helper._checkpointIdQueue.size());
    ASSERT_EQ(0, helper._pendingItemCount);

    notifier1.wait(-1);
    notifier2.wait(-1);
    notifier3.wait(-1);
}

TEST_F(SwiftWriterAsyncHelperTest, testCheckCallback_timeoutReady) {
    InnerMockSwiftWriterAsyncHelper helper;

    Notifier notifier1, notifier2, notifier3;
    notifier1.setNeedNotify();
    notifier2.setNeedNotify();
    notifier3.setNeedNotify();
    size_t callbackOrder = 0;
    {
        SwiftWriteCallbackItem item;
        item.payload->callback = [&](Result<SwiftWriteCallbackParam> result) {
            EXPECT_TRUE(result.is_err());
            notifier1.notify();
            EXPECT_EQ(3, ++callbackOrder);
        };
        item.expireTimeInUs = std::numeric_limits<int64_t>::max();
        helper._timeoutQueue.emplace(item);
    }
    {
        SwiftWriteCallbackItem item;
        item.payload->callback = [&](Result<SwiftWriteCallbackParam> result) {
            EXPECT_TRUE(result.is_err());
            notifier2.notify();
            EXPECT_EQ(2, ++callbackOrder);
        };
        item.expireTimeInUs = 2;
        helper._timeoutQueue.emplace(item);
    }
    {
        SwiftWriteCallbackItem item;
        item.payload->callback = [&](Result<SwiftWriteCallbackParam> result) {
            EXPECT_TRUE(result.is_err());
            notifier3.notify();
            EXPECT_EQ(1, ++callbackOrder);
        };
        item.expireTimeInUs = 1;
        helper._timeoutQueue.emplace(item);
    }
    helper._pendingItemCount = helper._timeoutQueue.size();

    EXPECT_CALL(helper, getCommittedCkptId()).WillRepeatedly(Return(100));

    helper.checkCallback();
    ASSERT_EQ(1, helper._timeoutQueue.size());
    ASSERT_EQ(1, helper._pendingItemCount);

    helper.checkCallback();
    ASSERT_EQ(1, helper._timeoutQueue.size());
    ASSERT_EQ(1, helper._pendingItemCount);

    (const_cast<SwiftWriteCallbackItem &>(helper._timeoutQueue.top())).expireTimeInUs = 0;
    helper.checkCallback();
    ASSERT_EQ(0, helper._timeoutQueue.size());
    ASSERT_EQ(0, helper._pendingItemCount);

    notifier1.wait(-1);
    notifier2.wait(-1);
    notifier3.wait(-1);
}

TEST_F(SwiftWriterAsyncHelperTest, testWaitEmpty) {
    SwiftWriterAsyncHelper helper;
    helper._pendingItemCount = 1;

    bool clearFlag = false;

    auto thread = autil::Thread::createThread([&]() {
        helper.waitEmpty();
        ASSERT_TRUE(clearFlag);
    });
    usleep(1 * 1000 * 1000);
    clearFlag = true;
    {
        ScopedLock _(helper._lock);
        helper._pendingItemCount = 0;
    }
    thread->join();
}

TEST_F(SwiftWriterAsyncHelperTest, testAll) {
    auto writerPtr = make_unique<MockSwiftWriter>();
    auto *writer = writerPtr.get();
    InnerMockSwiftWriterAsyncHelper helper;
    EXPECT_CALL(*writer, getCommittedCheckpointId()).WillOnce(Return(-1));
    EXPECT_CALL(*writer, setCommittedCallBack(_)).WillOnce(Return());
    EXPECT_CALL(helper, getCommittedCkptId()).WillOnce(Return(-1)).WillRepeatedly(Return(100));

    ASSERT_TRUE(helper.init(std::move(writerPtr), ""));

    EXPECT_CALL(*writer, write(_)).WillOnce(testing::Invoke([](MessageInfo &msgInfo) {
        msgInfo.checkpointId = 100;
        return ErrorCode::ERROR_NONE;
    }));
    EXPECT_CALL(helper, getCkptIdLimit()).WillOnce(Return(std::numeric_limits<int64_t>::max()));
    EXPECT_CALL(helper, stealRange(_, _)).WillOnce(testing::Invoke([](std::vector<int64_t> &timestamp, int64_t ckptId) {
        timestamp.resize(1);
    }));

    Notifier notifier;
    notifier.setNeedNotify();
    MessageInfo msgInfo;
    helper.write(
        &msgInfo,
        1,
        [&](Result<SwiftWriteCallbackParam> result) {
            EXPECT_TRUE(result.is_ok());
            notifier.notify();
        },
        std::numeric_limits<int64_t>::max());
    EXPECT_EQ(1, helper._checkpointIdAllocator);
    ASSERT_EQ(100, msgInfo.checkpointId);

    notifier.wait(-1);
    ASSERT_EQ(0, helper.getPendingItemCount());
}

}; // namespace client
}; // namespace swift
