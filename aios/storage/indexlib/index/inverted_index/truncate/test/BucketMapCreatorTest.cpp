#include "indexlib/index/inverted_index/truncate/BucketMapCreator.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/index/inverted_index/config/TruncateProfile.h"
#include "indexlib/index/inverted_index/config/TruncateProfileConfig.h"
#include "indexlib/index/inverted_index/config/TruncateStrategy.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class BucketMapCreatorTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}

private:
    std::shared_ptr<indexlibv2::config::TruncateStrategy> GetTruncateStrategy(std::string& content) const;
    std::shared_ptr<indexlibv2::config::TruncateProfile> GetTruncateProfile(std::string& content) const;
    std::shared_ptr<indexlibv2::config::ITabletSchema> GetDefaultSchema() const;
};

std::shared_ptr<indexlibv2::config::TruncateStrategy>
BucketMapCreatorTest::GetTruncateStrategy(std::string& content) const
{
    std::shared_ptr<indexlibv2::config::TruncateStrategy> strategy =
        std::make_shared<indexlibv2::config::TruncateStrategy>();
    auto ec = indexlib::file_system::JsonUtil::FromString(content, strategy.get()).Status();
    assert(ec.IsOK());
    return strategy;
}

std::shared_ptr<indexlibv2::config::TruncateProfile>
BucketMapCreatorTest::GetTruncateProfile(std::string& content) const
{
    std::shared_ptr<indexlibv2::config::TruncateProfileConfig> profileConfig =
        std::make_shared<indexlibv2::config::TruncateProfileConfig>();
    auto ec = indexlib::file_system::JsonUtil::FromString(content, profileConfig.get()).Status();
    assert(ec.IsOK());
    return std::make_shared<indexlibv2::config::TruncateProfile>(*profileConfig);
}

std::shared_ptr<indexlibv2::config::ITabletSchema> BucketMapCreatorTest::GetDefaultSchema() const
{
    std::string schemaStr = R"({
        "attributes": [
            "uvsum"
        ],
        "fields": [
            {
                "field_name": "nid",
                "field_type": "INTEGER"
            },
            {
                "field_name": "uvsum",
                "field_type": "INTEGER"
            }
        ],
        "indexs": [
            {
                "has_primary_key_attribute": true,
                "index_fields": "nid",
                "index_name": "pk",
                "index_type": "PRIMARYKEY64"
            }
        ],
        "table_name": "test"
    })";
    auto tabletSchema = std::make_shared<indexlibv2::config::TabletSchema>();
    assert(tabletSchema->Deserialize(schemaStr));
    return tabletSchema;
}

TEST_F(BucketMapCreatorTest, TestNeedCreateBucketMap)
{
    auto limitThreshold = std::numeric_limits<int>::max();
    std::string strategyStr = R"({
        "strategy_name": "static_trans_score_filter_by_meta",
        "truncate_profiles": [
            "desc_static_trans_score"
        ],
        "threshold": 90000,
        "memory_optimize_threshold": 20,
        "limit": 50000,
        "diversity_constrain": {
            "distinct_field": "user_id",
            "distinct_expand_limit": 120000,
            "distinct_count": 20000
        }
    })";

    auto strategyWithoutLimit = GetTruncateStrategy(strategyStr);
    ASSERT_TRUE(strategyWithoutLimit != nullptr);
    strategyWithoutLimit->_limit = limitThreshold;

    auto strategyWithLimit = GetTruncateStrategy(strategyStr);
    ASSERT_TRUE(strategyWithLimit != nullptr);

    std::string profileStr1 = R"({
        "sort_descriptions": "-uvsum;+nid",
        "truncate_profile_name": "desc_uvsum"
    })";
    // std::string profileStr2 = R"({
    //     "sort_descriptions": "-uvsum",
    //     "truncate_profile_name": "desc_uvsum"
    // })";
    auto profileWithoutSort = GetTruncateProfile(profileStr1);
    ASSERT_TRUE(profileWithoutSort != nullptr);
    profileWithoutSort->sortParams.clear();

    auto profileWithSort = GetTruncateProfile(profileStr1);
    ASSERT_TRUE(profileWithSort != nullptr);
    // auto profile = GetTruncateProfile(profileStr2);
    // ASSERT_TRUE(profile != nullptr);

    auto tabletSchema = GetDefaultSchema();
    ASSERT_TRUE(tabletSchema != nullptr);

    ASSERT_FALSE(BucketMapCreator::NeedCreateBucketMap(profileWithoutSort, /*strategy*/ nullptr,
                                                       /*tabletSchema*/ nullptr));
    ASSERT_FALSE(
        BucketMapCreator::NeedCreateBucketMap(profileWithSort, strategyWithoutLimit, /*tabletSchema*/ nullptr));
    // sort field is not an attribute
    ASSERT_FALSE(BucketMapCreator::NeedCreateBucketMap(profileWithSort, strategyWithLimit, tabletSchema));
    // 取正排config依赖表模型, 集成测试覆盖下面分支
    // ASSERT_TRUE(BucketMapCreator::NeedCreateBucketMap(profile, strategyWithLimit, tabletSchema));
}

} // namespace indexlib::index
