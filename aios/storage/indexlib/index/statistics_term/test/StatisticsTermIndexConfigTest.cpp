#include "indexlib/index/statistics_term/StatisticsTermIndexConfig.h"

#include "indexlib/index/statistics_term/Constant.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class StatisticsTermIndexConfigTest : public TESTBASE
{
};

TEST_F(StatisticsTermIndexConfigTest, testSimple)
{
    auto jsonStr = R"(
{
    "index_name" : "statistics_term",
    "index_type" : "statistics_term",
    "associated_inverted_index" : ["content_id1", "content_id2"]
}
)";
    auto config = std::make_unique<StatisticsTermIndexConfig>();
    ASSERT_NO_THROW(FromJsonString(*config, jsonStr));
    ASSERT_EQ("statistics_term", config->GetIndexName());
    EXPECT_THAT(config->GetInvertedIndexNames(), testing::UnorderedElementsAre("content_id1", "content_id2"));
    ASSERT_EQ(index::STATISTICS_TERM_INDEX_TYPE_STR, config->GetIndexType());
    EXPECT_THAT(config->GetIndexPath(), testing::UnorderedElementsAre("statistics_term/statistics_term"));

    ASSERT_TRUE(config->CheckCompatible(/*other*/ nullptr).IsOK());
    ASSERT_EQ(0, config->GetFieldConfigs().size());
}

} // namespace indexlibv2::index
