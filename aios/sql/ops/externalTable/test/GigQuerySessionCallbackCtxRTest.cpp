#include "sql/ops/externalTable/GigQuerySessionCallbackCtxR.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <type_traits>
#include <vector>

#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "aios/network/gig/multi_call/interface/SearchService.h"
#include "autil/result/Errors.h"
#include "autil/result/ForwardList.h"
#include "autil/result/Result.h"
#include "multi_call/common/ErrorCode.h"
#include "multi_call/common/common.h"
#include "multi_call/interface/Closure.h"
#include "multi_call/interface/Reply.h"
#include "multi_call/interface/Request.h"
#include "multi_call/interface/RequestGenerator.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/test/MockAsyncPipe.h"
#include "navi/resource/QuerySessionR.h"
#include "unittest/unittest.h"

namespace multi_call {
class Response;
} // namespace multi_call

using namespace std;
using namespace multi_call;
using namespace navi;
using namespace autil::result;
using namespace testing;

namespace sql {

class MockSearchService : public SearchService {
public:
    MOCK_METHOD3(search,
                 void(const SearchParam &param,
                      ReplyPtr &reply,
                      const QuerySessionPtr &querySession));
};

class MockRequestGenerator : public RequestGenerator {
public:
    MockRequestGenerator()
        : RequestGenerator("") {}

public:
    MOCK_METHOD2(generate, void(PartIdTy partCnt, PartRequestMap &requestMap));
};

class GigQuerySessionCallbackCtxRTest : public TESTBASE {
public:
    GigQuerySessionCallbackCtxRTest() {}
    ~GigQuerySessionCallbackCtxRTest() {}
};

TEST_F(GigQuerySessionCallbackCtxRTest, testOnWaitResponse) {
    {
        auto searchService = std::make_shared<MockSearchService>();
        auto querySessionR
            = std::make_shared<QuerySessionR>(std::make_shared<QuerySession>(searchService));
        auto pipe = std::make_shared<MockAsyncPipe>();
        GigQuerySessionCallbackCtxR ctx;
        ctx._asyncPipe = pipe;
        ctx._querySessionR = querySessionR.get();
        ctx.incStartVersion();
        EXPECT_CALL(*pipe, setData(_));
        ctx.onWaitResponse(RuntimeError::make("error"));
    }
    {
        auto searchService = std::make_shared<MockSearchService>();
        auto querySessionR
            = std::make_shared<QuerySessionR>(std::make_shared<QuerySession>(searchService));
        auto pipe = std::make_shared<MockAsyncPipe>();
        GigQuerySessionCallbackCtxR ctx;
        ctx._asyncPipe = pipe;
        ctx._querySessionR = querySessionR.get();
        std::vector<std::shared_ptr<multi_call::Response>> responses {nullptr};
        ctx.incStartVersion();
        EXPECT_CALL(*pipe, setData(_)).Times(1);
        ctx.onWaitResponse(responses);
        ASSERT_TRUE(ctx._result.is_ok());
        ASSERT_EQ(responses, ctx._result.get());
    }
}

TEST_F(GigQuerySessionCallbackCtxRTest, testStart) {
    {
        auto searchService = std::make_shared<MockSearchService>();
        auto querySessionR
            = std::make_shared<QuerySessionR>(std::make_shared<QuerySession>(searchService));
        auto pipe = std::make_shared<MockAsyncPipe>();
        GigQuerySessionCallbackCtxR ctx;
        ctx._asyncPipe = pipe;
        ctx._querySessionR = querySessionR.get();

        GigQuerySessionCallbackCtxR::ScanOptions options;
        options.requestGenerator = nullptr;
        ASSERT_FALSE(ctx.start(options));
    }
    {
        auto searchService = std::make_shared<MockSearchService>();
        auto querySessionR
            = std::make_shared<QuerySessionR>(std::make_shared<QuerySession>(searchService));
        auto pipe = std::make_shared<MockAsyncPipe>();
        auto ctx = std::make_shared<GigQuerySessionCallbackCtxR>(); // for shared_from_this
        ctx->_asyncPipe = pipe;
        ctx->_querySessionR = querySessionR.get();

        GigQuerySessionCallbackCtxR::ScanOptions options;
        options.requestGenerator = std::make_shared<MockRequestGenerator>();
        EXPECT_CALL(*searchService, search(_, _, _))
            .WillOnce(testing::Invoke([&](const SearchParam &param,
                                          ReplyPtr &reply,
                                          const QuerySessionPtr &querySession) {
                EXPECT_THAT(param.generatorVec, ElementsAre(options.requestGenerator));
                auto *closure = param.closure;
                reply = nullptr;
                closure->Run();
            }));
        ASSERT_TRUE(ctx->start(options));
    }
}

} // namespace sql
