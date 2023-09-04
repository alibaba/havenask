#include "sql/ops/scan/AsyncSummaryLookupCallbackCtx.h"

#include <algorithm>
#include <cstddef>
#include <exception>
#include <memory>
#include <utility>
#include <vector>

#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "future_lite/Try.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/indexlib.h"
#include "navi/common.h"
#include "navi/engine/AsyncPipe.h"
#include "navi/engine/Data.h"
#include "navi/engine/test/MockAsyncPipe.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace testing;

namespace sql {

class AsyncSummaryLookupCallbackCtxTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

private:
    autil::mem_pool::PoolPtr _poolPtr;
};

void AsyncSummaryLookupCallbackCtxTest::setUp() {
    _poolPtr.reset(new autil::mem_pool::Pool(1024));
}

void AsyncSummaryLookupCallbackCtxTest::tearDown() {}

TEST_F(AsyncSummaryLookupCallbackCtxTest, testCtor) {
    AsyncSummaryLookupCallbackCtx ctx({}, {}, NULL, _poolPtr, nullptr);
    ASSERT_EQ(false, ctx._hasError);
    ASSERT_EQ(0, ctx._startVersion);
    ASSERT_EQ(0, ctx._callbackVersion);
}

TEST_F(AsyncSummaryLookupCallbackCtxTest, testOnSessionCallback) {
    auto pipe = std::make_shared<navi::MockAsyncPipe>();
    CountedAsyncPipePtr countedPipe(new CountedAsyncPipe(pipe));
    AsyncSummaryLookupCallbackCtx ctx(countedPipe, {}, NULL, _poolPtr, nullptr);
    EXPECT_CALL(*pipe, setData(_)).WillOnce(Return(navi::EC_NONE));
    indexlib::index::ErrorCodeVec errorCodes;
    errorCodes.emplace_back(indexlib::index::ErrorCode::OK);
    future_lite::Try<indexlib::index::ErrorCodeVec> errorCodeTry(std::move(errorCodes));
    ctx._startVersion = 1;
    ctx._callbackVersion = 0;
    ctx.onSessionCallback(errorCodeTry);
    ASSERT_FALSE(ctx.hasError());
    ASSERT_EQ(0, ctx._errorDocIds.size());
}

TEST_F(AsyncSummaryLookupCallbackCtxTest, testOnSessionCallback_ErrorDocId) {
    auto pipe = std::make_shared<navi::MockAsyncPipe>();
    CountedAsyncPipePtr countedPipe(new CountedAsyncPipe(pipe));
    AsyncSummaryLookupCallbackCtx ctx(countedPipe, {}, NULL, _poolPtr, nullptr);
    EXPECT_CALL(*pipe, setData(_)).WillOnce(Return(navi::EC_NONE));
    ctx._docIds = {1, 2};
    indexlib::index::ErrorCodeVec errorCodes;
    errorCodes.emplace_back(indexlib::index::ErrorCode::OK);
    errorCodes.emplace_back(indexlib::index::ErrorCode::Timeout);
    future_lite::Try<indexlib::index::ErrorCodeVec> errorCodeTry(std::move(errorCodes));
    ctx._startVersion = 1;
    ctx._callbackVersion = 0;
    ctx.onSessionCallback(errorCodeTry);
    ASSERT_TRUE(ctx.hasError());
    ASSERT_EQ(1, ctx._errorDocIds.size());
    ASSERT_EQ(2, ctx._errorDocIds[0]);
}

TEST_F(AsyncSummaryLookupCallbackCtxTest, testOnSessionCallback_ErrorException) {
    auto pipe = std::make_shared<navi::MockAsyncPipe>();
    CountedAsyncPipePtr countedPipe(new CountedAsyncPipe(pipe));
    AsyncSummaryLookupCallbackCtx ctx(countedPipe, {}, NULL, _poolPtr, nullptr);
    EXPECT_CALL(*pipe, setData(_)).WillOnce(Return(navi::EC_NONE));
    future_lite::Try<indexlib::index::ErrorCodeVec> errorCodeTry;
    errorCodeTry.setException(make_exception_ptr(std::bad_exception()));
    ctx._startVersion = 1;
    ctx._callbackVersion = 0;
    ctx.onSessionCallback(errorCodeTry);
    ASSERT_TRUE(ctx.hasError());
    ASSERT_EQ(0, ctx._errorDocIds.size());
}

TEST_F(AsyncSummaryLookupCallbackCtxTest, testCountedAsyncPipeDone) {
    auto pipe = std::make_shared<navi::MockAsyncPipe>();
    size_t count = 3;
    CountedAsyncPipePtr countedPipe(new CountedAsyncPipe(pipe, count));
    for (size_t i = 0; i < count - 1; ++i) {
        ASSERT_EQ(navi::EC_NONE, countedPipe->done());
    }
    EXPECT_CALL(*pipe, setData(_)).WillOnce(Return(navi::EC_NONE));
    ASSERT_EQ(navi::EC_NONE, countedPipe->done());
}

} // namespace sql
