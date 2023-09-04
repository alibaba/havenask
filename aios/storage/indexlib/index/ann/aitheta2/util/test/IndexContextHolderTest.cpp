#include "indexlib/index/ann/aitheta2/util/AiThetaContextHolder.h"
#include "indexlib/index/ann/aitheta2/util/AithetaFactoryWrapper.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;
namespace indexlibv2::index::ann {

class IndexContextHolderTest : public TESTBASE
{
public:
    void testGeneral();
};

TEST_F(IndexContextHolderTest, TestGeneral)
{
    indexlib::util::KeyValueMap keyValueMap = {{INDEX_BUILDER_NAME, "LinearBuilder"},
                                               {INDEX_SEARCHER_NAME, "LinearSearcher"},
                                               {DISTANCE_TYPE, "SquaredEuclidean"},
                                               {INDEX_STREAMER_NAME, "OswgStreamer"},
                                               {DIMENSION, "128"},
                                               {INDEX_ORDER_TYPE, ORDER_TYPE_COL},
                                               {INDEX_PARAMETERS, "{}"}};
    AithetaIndexConfig indexConfig(keyValueMap);

    std::shared_ptr<RealtimeIndexBuildResource> resource;
    AiThetaStreamerPtr streamer;
    ASSERT_TRUE(AiThetaFactoryWrapper::CreateStreamer(indexConfig, resource, streamer));
    AiThetaContextHolder holder;

    auto [isNew, ctx] = holder.CreateIfNotExist(streamer, "OswgStreamer", "");
    ASSERT_TRUE(isNew);
    ASSERT_NE(nullptr, ctx);

    auto oldCtx = ctx;

    std::tie(isNew, ctx) = holder.CreateIfNotExist(streamer, "OswgStreamer", "");
    ASSERT_FALSE(isNew);
    ASSERT_EQ(oldCtx, ctx);

    std::tie(isNew, ctx) = holder.CreateIfNotExist(streamer, "OswgStreamer", "{}");
    ASSERT_TRUE(isNew);
    ASSERT_NE(oldCtx, ctx);
}

} // namespace indexlibv2::index::ann
