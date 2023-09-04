#include <algorithm>
#include <cstddef>
#include <memory>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "future_lite/CoroInterface.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/kv/KVIndexReader.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/test/MockAsyncPipe.h"
#include "sql/ops/scan/AsyncKVLookupCallbackCtxV2.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace testing;
using namespace indexlibv2::index;

namespace sql {

class AsyncKVLookupCallbackCtxV2Test : public TESTBASE {
public:
    void setUp();
    void tearDown();

private:
    autil::mem_pool::PoolPtr _poolPtr;
};

void AsyncKVLookupCallbackCtxV2Test::setUp() {
    _poolPtr.reset(new autil::mem_pool::Pool(1024));
}

void AsyncKVLookupCallbackCtxV2Test::tearDown() {}

TEST_F(AsyncKVLookupCallbackCtxV2Test, testCtor) {
    AsyncKVLookupCallbackCtxV2 ctx({}, nullptr);
    ASSERT_EQ(0, ctx._startVersion);
    ASSERT_EQ(0, ctx._callbackVersion);
}

TEST_F(AsyncKVLookupCallbackCtxV2Test, testOnSessionCallback) {
    auto pipe = std::make_shared<navi::MockAsyncPipe>();
    AsyncKVLookupCallbackCtxV2 ctx(pipe, nullptr);
    EXPECT_CALL(*pipe, setData(_)).WillOnce(Return(navi::EC_NONE));
    ctx._rawPks = {"1"};
    ctx.preparePksForSearch();
    AsyncKVLookupCallbackCtxV2::StatusVector statusVec(_poolPtr.get());
    statusVec.emplace_back(KVResultStatus::FOUND);
    future_lite::interface::use_try_t<AsyncKVLookupCallbackCtxV2::StatusVector> statusVecTry(
        std::move(statusVec));
    ctx._startVersion = 1;
    ctx._callbackVersion = 0;
    ctx.onSessionCallback(std::move(statusVecTry));
    ASSERT_EQ(0, ctx._failedPks.size());
}

TEST_F(AsyncKVLookupCallbackCtxV2Test, testOnSessionCallback_HasFailedDocId) {
    auto pipe = std::make_shared<navi::MockAsyncPipe>();
    AsyncKVLookupCallbackCtxV2 ctx(pipe, nullptr);
    EXPECT_CALL(*pipe, setData(_)).WillOnce(Return(navi::EC_NONE));
    ctx._rawPks = {"1", "2", "3", "4"};
    ctx.preparePksForSearch();
    AsyncKVLookupCallbackCtxV2::StatusVector statusVec(_poolPtr.get());
    statusVec.emplace_back(KVResultStatus::FOUND);
    statusVec.emplace_back(KVResultStatus::NOT_FOUND);
    statusVec.emplace_back(KVResultStatus::TIMEOUT);
    statusVec.emplace_back(KVResultStatus::DELETED);
    future_lite::interface::use_try_t<AsyncKVLookupCallbackCtxV2::StatusVector> statusVecTry(
        std::move(statusVec));
    ctx._startVersion = 1;
    ctx._callbackVersion = 0;
    ctx.onSessionCallback(std::move(statusVecTry));
    ASSERT_THAT(ctx._failedPks, ElementsAre(Pair("3", (uint32_t)KVResultStatus::TIMEOUT)));
    ASSERT_THAT(ctx._notFoundPks, ElementsAre("2", "4"));
}

// ================= test only for coroutine mode begin ===============
#ifdef FUTURE_LITE_USE_COROUTINES

TEST_F(AsyncKVLookupCallbackCtxV2Test, DISABLED_testOnSessionCallback_ErrorException) {
    auto pipe = std::make_shared<navi::MockAsyncPipe>();
    AsyncKVLookupCallbackCtxV2 ctx(pipe, nullptr);
    EXPECT_CALL(*pipe, setData(_)).WillOnce(Return(navi::EC_NONE));
    AsyncKVLookupCallbackCtxV2::StatusVector statusVec(_poolPtr.get());
    future_lite::interface::use_try_t<AsyncKVLookupCallbackCtxV2::StatusVector> statusVecTry(
        std::move(statusVec));
    statusVecTry.setException(make_exception_ptr(std::bad_exception()));
    ctx._startVersion = 1;
    ctx._callbackVersion = 0;
    ctx.onSessionCallback(std::move(statusVecTry));
    ASSERT_EQ(0, ctx._failedPks.size());
}

#endif
// ================= test only for coroutine mode end ===============

} // namespace sql
