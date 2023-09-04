#include "indexlib/index/inverted_index/truncate/TruncateTriggerCreator.h"

#include "indexlib/file_system/JsonUtil.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class TruncateTriggerCreatorTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}

private:
    std::shared_ptr<indexlibv2::config::TruncateStrategy> GetTruncateStrategy(std::string& content) const;
};

std::shared_ptr<indexlibv2::config::TruncateStrategy>
TruncateTriggerCreatorTest::GetTruncateStrategy(std::string& content) const
{
    auto strategy = std::make_shared<indexlibv2::config::TruncateStrategy>();
    auto ec = indexlib::file_system::JsonUtil::FromString(content, strategy.get()).Status();
    assert(ec.IsOK());
    return strategy;
}

TEST_F(TruncateTriggerCreatorTest, TestSimpleProcess)
{
    {
        std::string str = R"({
            "strategy_name": "strategy1",
            "strategy_type": "truncate_meta",
            "threshold": 90000,
            "limit": 50000
        })";
        auto truncateStrategy = GetTruncateStrategy(str);

        indexlibv2::config::TruncateIndexProperty property;
        property.truncateStrategy = truncateStrategy;

        auto truncateTriggerCreator = std::make_shared<TruncateTriggerCreator>();
        auto trigger = truncateTriggerCreator->Create(property);
        assert(trigger);
    }
    {
        std::string str = R"({
        "strategy_name": "static_trans_score_filter_by_meta",
        "strategy_type": "default",
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
        auto truncateStrategy = GetTruncateStrategy(str);
        indexlibv2::config::TruncateIndexProperty property;
        property.truncateStrategy = truncateStrategy;

        auto truncateTriggerCreator = std::make_shared<TruncateTriggerCreator>();
        auto trigger = truncateTriggerCreator->Create(property);
        assert(trigger);
    }
}
} // namespace indexlib::index
