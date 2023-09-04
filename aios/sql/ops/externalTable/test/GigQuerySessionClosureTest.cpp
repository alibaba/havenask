#include "sql/ops/externalTable/GigQuerySessionClosure.h"

#include <cstddef>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "aios/network/gig/multi_call/interface/Reply.h"
#include "autil/result/ForwardList.h"
#include "autil/result/Result.h"
#include "multi_call/common/ErrorCode.h"
#include "multi_call/interface/Response.h"
#include "sql/ops/externalTable/GigQuerySessionCallbackCtxR.h"
#include "unittest/unittest.h"

using namespace std;
using namespace multi_call;
using namespace autil::result;
using namespace testing;

namespace sql {

class MockReply : public Reply {
public:
    MOCK_METHOD2(getResponses,
                 std::vector<ResponsePtr>(size_t &lackCount,
                                          std::vector<LackResponseInfo> &lackInfos));
};

class MockGigQuerySessionCallbackCtxR : public GigQuerySessionCallbackCtxR {
public:
    MockGigQuerySessionCallbackCtxR()
        : GigQuerySessionCallbackCtxR() {}

public:
    MOCK_METHOD1(onWaitResponse,
                 void(autil::Result<GigQuerySessionCallbackCtxR::ResponseVec> result));
};

class GigQuerySessionClosureTest : public TESTBASE {
public:
    GigQuerySessionClosureTest() {}
    ~GigQuerySessionClosureTest() {}
};

TEST_F(GigQuerySessionClosureTest, testDoRunErr) {
    {
        autil::Result<GigQuerySessionCallbackCtxR::ResponseVec> result;
        auto ctx = std::make_shared<MockGigQuerySessionCallbackCtxR>();
        GigQuerySessionClosure closure(ctx);
        EXPECT_CALL(*ctx, onWaitResponse(_))
            .WillOnce(testing::Invoke(
                [&result](autil::Result<GigQuerySessionCallbackCtxR::ResponseVec> res) {
                    result = std::move(res);
                }));
        closure.doRun();
        ASSERT_TRUE(result.is_err());
        ASSERT_EQ("gig reply is null", result.get_error().message());
    }
    {
        autil::Result<GigQuerySessionCallbackCtxR::ResponseVec> result;
        auto ctx = std::make_shared<MockGigQuerySessionCallbackCtxR>();
        GigQuerySessionClosure closure(ctx);
        EXPECT_CALL(*ctx, onWaitResponse(_))
            .WillOnce(testing::Invoke(
                [&result](autil::Result<GigQuerySessionCallbackCtxR::ResponseVec> res) {
                    result = std::move(res);
                }));
        auto reply = std::make_shared<MockReply>();
        closure._reply = reply;
        EXPECT_CALL(*reply, getResponses(_, _))
            .WillOnce(
                testing::Invoke([](size_t &lackCount, std::vector<LackResponseInfo> &lackInfos) {
                    lackCount = 1;
                    return GigQuerySessionCallbackCtxR::ResponseVec {};
                }));
        closure.doRun();
        ASSERT_TRUE(result.is_err());
        ASSERT_EQ("lack provider, count [1], lack info []", result.get_error().message());
    }
}

TEST_F(GigQuerySessionClosureTest, testDoRun) {
    autil::Result<GigQuerySessionCallbackCtxR::ResponseVec> result;
    auto ctx = std::make_shared<MockGigQuerySessionCallbackCtxR>();
    GigQuerySessionClosure closure(ctx);
    EXPECT_CALL(*ctx, onWaitResponse(_))
        .WillOnce(
            testing::Invoke([&result](autil::Result<GigQuerySessionCallbackCtxR::ResponseVec> res) {
                result = std::move(res);
            }));
    auto reply = std::make_shared<MockReply>();
    closure._reply = reply;
    EXPECT_CALL(*reply, getResponses(_, _))
        .WillOnce(testing::Invoke([](size_t &lackCount, std::vector<LackResponseInfo> &lackInfos) {
            std::vector<std::shared_ptr<multi_call::Response>> res {nullptr};
            return res;
        }));
    closure.doRun();
    ASSERT_TRUE(result.is_ok());
    ASSERT_THAT(result.get(), ElementsAre(nullptr));
}

} // namespace sql
