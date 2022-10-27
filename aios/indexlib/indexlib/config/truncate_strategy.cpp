#include "indexlib/config/truncate_strategy.h"
#include "indexlib/config/configurator_define.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, TruncateStrategy);

const uint64_t TruncateStrategy::MAX_TRUNC_THRESHOLD = 
    std::numeric_limits<df_t>::max();
const uint64_t TruncateStrategy::MAX_TRUNC_LIMIT = 
    std::numeric_limits<df_t>::max();
const int32_t TruncateStrategy::MAX_MEMORY_OPTIMIZE_PERCENTAGE = 100;
const int32_t TruncateStrategy::DEFAULT_MEMORY_OPTIMIZE_PERCENTAGE = 20;

TruncateStrategy::TruncateStrategy()
    : mStrategyName("")
    , mStrategyType(DEFAULT_TRUNCATE_STRATEGY_TYPE)
    , mThreshold(0)
    , mMemoryOptimizeThreshold(DEFAULT_MEMORY_OPTIMIZE_PERCENTAGE)
    , mLimit(MAX_TRUNC_LIMIT)
{
}

TruncateStrategy::~TruncateStrategy() 
{
}

void TruncateStrategy::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("strategy_name", mStrategyName, mStrategyName);
    json.Jsonize("strategy_type", mStrategyType, mStrategyType);
    json.Jsonize("threshold", mThreshold, mThreshold);
    json.Jsonize("memory_optimize_threshold", mMemoryOptimizeThreshold, 
                 mMemoryOptimizeThreshold);
    json.Jsonize("limit", mLimit, mLimit);
    json.Jsonize("diversity_constrain", mDiversityConstrain, 
                 mDiversityConstrain);
    json.Jsonize("truncate_profiles", mTruncateProfileNames, mTruncateProfileNames);
}

bool TruncateStrategy::HasLimit() const
{
    return mLimit != MAX_TRUNC_LIMIT;
}

void TruncateStrategy::Check() const 
{
    IE_CONFIG_ASSERT_PARAMETER_VALID(!mStrategyName.empty(), 
            "StrategyName should not be null");
    bool valid = mStrategyType == DEFAULT_TRUNCATE_STRATEGY_TYPE || 
                 mStrategyType == TRUNCATE_META_STRATEGY_TYPE;
    IE_CONFIG_ASSERT_PARAMETER_VALID(valid, "invalid strategy type");

    IE_CONFIG_ASSERT_PARAMETER_VALID(mLimit > 0, 
            "Limit should be larger than zero");

    if (mDiversityConstrain.NeedDistinct())
    {
        bool valid = mDiversityConstrain.GetDistCount() <= mLimit;
        IE_CONFIG_ASSERT_PARAMETER_VALID(valid, 
                "mDistCount should be less than mLimit");
        valid = mDiversityConstrain.GetDistExpandLimit() >= mLimit;
        IE_CONFIG_ASSERT_PARAMETER_VALID(valid, 
                "mDistExpandLimit should be greater than mLimit");
    }
    mDiversityConstrain.Check();

    valid = mMemoryOptimizeThreshold <= MAX_MEMORY_OPTIMIZE_PERCENTAGE
            && mMemoryOptimizeThreshold >= 0;
    IE_CONFIG_ASSERT_PARAMETER_VALID(valid,
            "MemoryOptimizeThreshold should be [0~100]");
} 

bool TruncateStrategy::operator==(const TruncateStrategy& other) const
{
    return mStrategyName == other.mStrategyName
        && mStrategyType == other.mStrategyType
        && mThreshold == other.mThreshold
        && mMemoryOptimizeThreshold == other.mMemoryOptimizeThreshold
        && mLimit == other.mLimit
        && mDiversityConstrain == other.mDiversityConstrain;
}

IE_NAMESPACE_END(config);

