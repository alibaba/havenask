#include "indexlib/index/inverted_index/config/TruncateStrategy.h"

#include <string>

#include "indexlib/file_system/JsonUtil.h"
#include "unittest/unittest.h"

namespace indexlibv2::config {

class TruncateStrategyTest : public TESTBASE
{
    void setUp() override {}
    void tearDown() override {}

private:
    std::shared_ptr<TruncateStrategy> GetTruncateStrategy(std::string& content) const;
};

std::shared_ptr<TruncateStrategy> TruncateStrategyTest::GetTruncateStrategy(std::string& content) const
{
    std::shared_ptr<TruncateStrategy> strategy = std::make_shared<TruncateStrategy>();
    auto ec = indexlib::file_system::JsonUtil::FromString(content, strategy.get()).Status();
    assert(ec.IsOK());
    return strategy;
}

TEST_F(TruncateStrategyTest, TestSimpleProcess)
{
    std::string str = R"({
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
    std::shared_ptr<TruncateStrategy> truncateStrategy = GetTruncateStrategy(str);
    ASSERT_EQ("static_trans_score_filter_by_meta", truncateStrategy->GetStrategyName());
    ASSERT_EQ("default", truncateStrategy->GetStrategyType());
    ASSERT_EQ(20, truncateStrategy->GetMemoryOptimizeThreshold());
    ASSERT_EQ(50000, truncateStrategy->GetLimit());
    ASSERT_EQ(90000, truncateStrategy->GetThreshold());
    auto profileNames = truncateStrategy->GetProfileNames();
    ASSERT_EQ("desc_static_trans_score", profileNames[0]);
}

TEST_F(TruncateStrategyTest, TestCheck)
{
    {
        std::string str = R"({
            "threshold": 90000
        })";
        std::shared_ptr<TruncateStrategy> truncateStrategy = GetTruncateStrategy(str);
        ASSERT_FALSE(truncateStrategy->Check());
    }
    {
        std::string str = R"({
            "strategy_type": "wrong_type"
        })";
        std::shared_ptr<TruncateStrategy> truncateStrategy = GetTruncateStrategy(str);
        ASSERT_FALSE(truncateStrategy->Check());
    }
    {
        std::string str = R"({
            "limit": 0
        })";
        std::shared_ptr<TruncateStrategy> truncateStrategy = GetTruncateStrategy(str);
        ASSERT_FALSE(truncateStrategy->Check());
    }
    {
        std::string str = R"({
            "strategy_name": "static_trans_score_filter_by_meta",
            "threshold": 90000,
            "limit": 10000,
            "diversity_constrain": {
                "distinct_field": "user_id",
                "distinct_expand_limit": 120000,
                "distinct_count": 20000
            }
        })";
        std::shared_ptr<TruncateStrategy> truncateStrategy = GetTruncateStrategy(str);
        ASSERT_FALSE(truncateStrategy->Check());
    }
    {
        std::string str = R"({
            "strategy_name": "static_trans_score_filter_by_meta",
            "threshold": 90000,
            "limit": 20000,
            "diversity_constrain": {
                "distinct_field": "user_id",
                "distinct_expand_limit": 10000,
                "distinct_count": 20000
            }
        })";
        std::shared_ptr<TruncateStrategy> truncateStrategy = GetTruncateStrategy(str);
        ASSERT_FALSE(truncateStrategy->Check());
    }
    {
        std::string str = R"({
            "strategy_name": "static_trans_score_filter_by_meta",
            "memory_optimize_threshold": 120
        })";
        std::shared_ptr<TruncateStrategy> truncateStrategy = GetTruncateStrategy(str);
        ASSERT_FALSE(truncateStrategy->Check());
    }
}

} // namespace indexlibv2::config
