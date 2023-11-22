#include "swift/client/helper/FixedTimeoutSwiftWriterAsyncHelper.h"

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

namespace swift::client {

class InnerMockFixedTimeoutSwiftWriterAsyncHelper : public swift::client::FixedTimeoutSwiftWriterAsyncHelper {
private:
    MOCK_CONST_METHOD0(getCommittedCkptId, int64_t(void));
    MOCK_CONST_METHOD0(getCkptIdLimit, int64_t(void));
    MOCK_METHOD2(stealRange, void(std::vector<int64_t> &timestamps, int64_t ckptId));
};

class FixedTimeoutSwiftWriterAsyncHelperTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void FixedTimeoutSwiftWriterAsyncHelperTest::setUp() {}

void FixedTimeoutSwiftWriterAsyncHelperTest::tearDown() {}

TEST_F(FixedTimeoutSwiftWriterAsyncHelperTest, testInit) {
    auto writerPtr = make_unique<MockSwiftWriter>();
    auto *writer = writerPtr.get();
    EXPECT_CALL(*writer, getCommittedCheckpointId()).WillOnce(Return(-1)).WillRepeatedly(Return(0));
    EXPECT_CALL(*writer, setCommittedCallBack(_)).WillOnce(Return());
    FixedTimeoutSwiftWriterAsyncHelper helper;
    ASSERT_TRUE(helper.init(std::move(writerPtr), "test-fixed", 1000 * 1000));
    usleep(1 * 1000 * 1000);
    ASSERT_EQ(helper._timeoutInUs, 1000 * 1000);
}

TEST_F(FixedTimeoutSwiftWriterAsyncHelperTest, testWrite_Success_NoTimeout) {
    auto writerPtr = make_unique<MockSwiftWriter>();
    auto *writer = writerPtr.get();
    std::unique_ptr<InnerMockFixedTimeoutSwiftWriterAsyncHelper> helperHolder;
    helperHolder.reset(new InnerMockFixedTimeoutSwiftWriterAsyncHelper);
    InnerMockFixedTimeoutSwiftWriterAsyncHelper& helper = *helperHolder.get();
    helper._writer = std::move(writerPtr);
    helper._checkpointIdAllocator = 0;
    helper._timeoutInUs = std::numeric_limits<int64_t>::max();

    EXPECT_CALL(helper, getCkptIdLimit()).WillOnce(Return(std::numeric_limits<int64_t>::max()));
    EXPECT_CALL(*writer, write(_)).WillOnce(Return(ErrorCode::ERROR_NONE)).RetiresOnSaturation();

    MessageInfo msgInfo;
    Notifier notifier;
    notifier.setNeedNotify();
    helper.write(&msgInfo, 1, [&](Result<SwiftWriteCallbackParam> result) {
        EXPECT_TRUE(result.is_err());
        notifier.notify();
    });
    EXPECT_EQ(1, helper._checkpointIdAllocator);
    ASSERT_EQ(1, helper._checkpointIdQueue.size());
    ASSERT_EQ(0, helper._fixedTimeoutQueue.size());
    ASSERT_EQ(0, helper._timeoutQueue.size());
    ASSERT_EQ(1, helper._pendingItemCount);
    auto &item = helper._checkpointIdQueue.front();
    ASSERT_TRUE(item.payload->callback);
    ASSERT_EQ(0, item.checkpointId);
    ASSERT_GE(std::numeric_limits<int64_t>::max(), item.expireTimeInUs);
    helperHolder.reset();
    notifier.wait(-1);
}

TEST_F(FixedTimeoutSwiftWriterAsyncHelperTest, testCheckCallback_timeoutReady) {
    InnerMockFixedTimeoutSwiftWriterAsyncHelper helper;

    Notifier notifier1, notifier2, notifier3;
    notifier1.setNeedNotify();
    notifier2.setNeedNotify();
    notifier3.setNeedNotify();
    size_t callbackOrder = 0;
    {
        SwiftWriteCallbackItem item;
        item.payload->callback = [&](Result<SwiftWriteCallbackParam> result) {
            EXPECT_TRUE(result.is_err());
            notifier3.notify();
            EXPECT_EQ(1, ++callbackOrder);
        };
        item.expireTimeInUs = 1;
        helper._fixedTimeoutQueue.emplace_back(item);
    }
    {
        SwiftWriteCallbackItem item;
        item.payload->callback = [&](Result<SwiftWriteCallbackParam> result) {
            EXPECT_TRUE(result.is_err());
            notifier2.notify();
            EXPECT_EQ(2, ++callbackOrder);
        };
        item.expireTimeInUs = 2;
        helper._fixedTimeoutQueue.emplace_back(item);
    }
    {
        SwiftWriteCallbackItem item;
        item.payload->callback = [&](Result<SwiftWriteCallbackParam> result) {
            EXPECT_TRUE(result.is_err());
            notifier1.notify();
            EXPECT_EQ(3, ++callbackOrder);
        };
        item.expireTimeInUs = std::numeric_limits<int64_t>::max();
        helper._fixedTimeoutQueue.emplace_back(item);
    }

    helper._pendingItemCount = helper._fixedTimeoutQueue.size();

    EXPECT_CALL(helper, getCommittedCkptId()).WillRepeatedly(Return(100));

    helper.checkCallback();
    ASSERT_EQ(0, helper._timeoutQueue.size());
    ASSERT_EQ(1, helper._fixedTimeoutQueue.size());
    ASSERT_EQ(1, helper._pendingItemCount);

    helper.checkCallback();
    ASSERT_EQ(0, helper._timeoutQueue.size());
    ASSERT_EQ(1, helper._fixedTimeoutQueue.size());
    ASSERT_EQ(1, helper._pendingItemCount);

    (const_cast<SwiftWriteCallbackItem &>(helper._fixedTimeoutQueue.front())).expireTimeInUs = 0;
    helper.checkCallback();
    ASSERT_EQ(0, helper._timeoutQueue.size());
    ASSERT_EQ(0, helper._fixedTimeoutQueue.size());
    ASSERT_EQ(0, helper._pendingItemCount);

    notifier1.wait(-1);
    notifier2.wait(-1);
    notifier3.wait(-1);
}

TEST_F(FixedTimeoutSwiftWriterAsyncHelperTest, testAll) {
    auto writerPtr = make_unique<MockSwiftWriter>();
    auto *writer = writerPtr.get();
    InnerMockFixedTimeoutSwiftWriterAsyncHelper helper;
    EXPECT_CALL(*writer, getCommittedCheckpointId()).WillOnce(Return(-1));
    EXPECT_CALL(*writer, setCommittedCallBack(_)).WillOnce(Return());
    EXPECT_CALL(helper, getCommittedCkptId()).WillOnce(Return(-1)).WillRepeatedly(Return(100));

    ASSERT_TRUE(helper.init(std::move(writerPtr), "test-fixed", 1000 * 1000));

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
        });
    EXPECT_EQ(1, helper._checkpointIdAllocator);
    ASSERT_EQ(100, msgInfo.checkpointId);
    EXPECT_EQ(1, helper._fixedTimeoutQueue.size());
    EXPECT_EQ(0, helper._timeoutQueue.size());

    notifier.wait(-1);
    ASSERT_EQ(0, helper.getPendingItemCount());
}

}; // namespace swift::client
