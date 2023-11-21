#include "suez/table/wal/RealtimeSwiftWAL.h"

#include <atomic>
#include <condition_variable>
#include <mutex>

#include "autil/Thread.h"
#include "autil/result/Errors.h"
#include "swift/testlib/MockSwiftWriterAsyncHelper.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil::result;
using namespace suez;
using namespace swift::testlib;
using namespace swift::common;
using namespace swift::client;

class RealtimeSwiftWALTest : public TESTBASE {
public:
    void setUp() override {}
    RealtimeSwiftWAL wal;
    Result<vector<int64_t>> ret;
    bool isReturned = false;
    std::function<void(Result<std::vector<int64_t>>)> done;
};

TEST_F(RealtimeSwiftWALTest, testStop) {
    auto wal = RealtimeSwiftWAL();
    wal._swiftWriterAsyncHelper.reset(new MockFixedTimeoutSwiftWriterAsyncHelper);
    wal.stop();
}

TEST_F(RealtimeSwiftWALTest, testLogAfterStop) {
    auto wal = RealtimeSwiftWAL();
    wal._swiftWriterAsyncHelper.reset(new MockFixedTimeoutSwiftWriterAsyncHelper);
    wal.stop();

    Result<vector<int64_t>> ret;
    auto done = [&ret](Result<vector<int64_t>> result) { ret = std::move(result); };
    wal.log({{0, "nid=3"}}, done);
    ASSERT_FALSE(ret.is_ok());
}

TEST_F(RealtimeSwiftWALTest, testStopWhileLog) {
    auto wal = RealtimeSwiftWAL();
    auto *mockSwiftHelper = new MockFixedTimeoutSwiftWriterAsyncHelper;
    wal._swiftWriterAsyncHelper.reset(mockSwiftHelper);

    mutex mu;
    condition_variable cv;
    bool isStart = false;

    std::vector<int64_t> timestamps;

    EXPECT_CALL(*mockSwiftHelper, write(_, _, _))
        .WillOnce(testing::Invoke([&](MessageInfo *msgInfos,
                                      size_t count,
                                      SwiftWriteCallbackItem::WriteCallbackFunc callback
                                      ) {
            {
                unique_lock<mutex> lock(mu);
                isStart = true;
            }
            cv.notify_all();
            usleep(2 * 1000 * 1000);
            timestamps.resize(count);
            callback(SwiftWriteCallbackParam{timestamps.data(), count});
        }));

    Result<std::vector<int64_t>> ret = RuntimeError::make("not init");
    auto done = [&ret](Result<std::vector<int64_t>> result) { ret = std::move(result); };
    auto thread = autil::Thread::createThread([&]() { wal.log({{0, "nid=3"}}, done); });
    {
        unique_lock<mutex> lock(mu);
        cv.wait(lock, [&isStart]() { return isStart; });
    }
    ASSERT_FALSE(ret.is_ok());
    wal.stop();
    ASSERT_TRUE(ret.is_ok());
    thread->join();
}

TEST_F(RealtimeSwiftWALTest, testLogSuccess) {
    auto wal = RealtimeSwiftWAL();
    auto *mockSwiftHelper = new MockFixedTimeoutSwiftWriterAsyncHelper;
    wal._swiftWriterAsyncHelper.reset(mockSwiftHelper);

    std::vector<int64_t> timestamps;
    EXPECT_CALL(*mockSwiftHelper, write(_, _, _))
        .WillRepeatedly(testing::Invoke([&](MessageInfo *msgInfos,
                                            size_t count,
                                            SwiftWriteCallbackItem::WriteCallbackFunc callback) {
            timestamps.resize(count);
            callback(SwiftWriteCallbackParam{timestamps.data(), count});
        }));

    mutex mu;
    condition_variable cv;
    Result<std::vector<int64_t>> ret;
    atomic<bool> isReturn = false;

    auto done = [&ret, &cv, &isReturn](Result<std::vector<int64_t>> result) {
        ret = std::move(result);
        isReturn = true;
        cv.notify_all();
    };
    wal.log({{0, "nid=3"}}, done);
    {
        unique_lock<mutex> lock(mu);
        cv.wait(lock, [&isReturn]() { return isReturn == true; });
        ASSERT_TRUE(ret.is_ok());
    }
    wal.log({{1, "nid=3"}}, done);
    {
        unique_lock<mutex> lock(mu);
        cv.wait(lock, [&isReturn]() { return isReturn == true; });
        ASSERT_TRUE(ret.is_ok());
    }
    wal.log({{2, "nid=3"}}, done);
    {
        unique_lock<mutex> lock(mu);
        cv.wait(lock, [&isReturn]() { return isReturn == true; });
        ASSERT_TRUE(ret.is_ok());
    }
}

TEST_F(RealtimeSwiftWALTest, testLogFailed) {
    auto wal = RealtimeSwiftWAL();
    auto *mockSwiftHelper = new MockFixedTimeoutSwiftWriterAsyncHelper;
    wal._swiftWriterAsyncHelper.reset(mockSwiftHelper);

    EXPECT_CALL(*mockSwiftHelper, write(_, _, _))
        .WillOnce(testing::Invoke([](MessageInfo *msgInfos,
                                     size_t count,
                                     SwiftWriteCallbackItem::WriteCallbackFunc callback) { callback(RuntimeError::make("mock write has error")); }));

    Result<vector<int64_t>> ret;
    auto done = [&ret](Result<vector<int64_t>> result) { ret = std::move(result); };
    wal.log({{0, "nid=3"}}, done);
    ASSERT_FALSE(ret.is_ok());
}
