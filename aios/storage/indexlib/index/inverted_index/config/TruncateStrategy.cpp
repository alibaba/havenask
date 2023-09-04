#include "indexlib/index/inverted_index/config/TruncateStrategy.h"

namespace indexlibv2::config {

AUTIL_LOG_SETUP(indexlib.config, TruncateStrategy);

TruncateStrategy::TruncateStrategy()
    : _strategyName("")
    , _strategyType("default")
    , _threshold(0)
    , _memoryOptimizeThreshold(DEFAULT_MEMORY_OPTIMIZE_PERCENTAGE)
    , _limit(MAX_TRUNC_LIMIT)
{
}

void TruncateStrategy::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("strategy_name", _strategyName, _strategyName);
    json.Jsonize("strategy_type", _strategyType, _strategyType);
    json.Jsonize("threshold", _threshold, _threshold);
    json.Jsonize("memory_optimize_threshold", _memoryOptimizeThreshold, _memoryOptimizeThreshold);
    json.Jsonize("limit", _limit, _limit);
    json.Jsonize("diversity_constrain", _diversityConstrain, _diversityConstrain);
    json.Jsonize("truncate_profiles", _truncateProfileNames, _truncateProfileNames);
}

bool TruncateStrategy::HasLimit() const { return _limit != MAX_TRUNC_LIMIT; }

bool TruncateStrategy::Check() const
{
    if (_strategyName.empty()) {
        AUTIL_LOG(ERROR, "strategy name should't be empty");
        return false;
    }
    if (_strategyType != "default" && _strategyType != "truncate_meta") {
        AUTIL_LOG(ERROR, "invalid strategy type:%s", _strategyType.c_str());
        return false;
    }
    if (_limit <= 0) {
        AUTIL_LOG(ERROR, "limit shoud be greater than 0.");
        return false;
    }

    if (_diversityConstrain.NeedDistinct()) {
        if (_diversityConstrain.GetDistCount() > _limit) {
            AUTIL_LOG(ERROR, "distCount[%lu] in constrain should be less than limit[%lu]",
                      _diversityConstrain.GetDistCount(), _limit);
            return false;
        }

        if (_diversityConstrain.GetDistExpandLimit() < _limit) {
            AUTIL_LOG(ERROR, "distExpandLimit[%lu] should be greater than limit[%lu]",
                      _diversityConstrain.GetDistExpandLimit(), _limit);
            return false;
        }
    }
    _diversityConstrain.Check();

    if (_memoryOptimizeThreshold > MAX_MEMORY_OPTIMIZE_PERCENTAGE || _memoryOptimizeThreshold < 0) {
        AUTIL_LOG(ERROR, "memoryOptimizeThreshold should be [0~100]");
        return false;
    }
    return true;
}

bool TruncateStrategy::operator==(const TruncateStrategy& other) const
{
    return _strategyName == other._strategyName && _strategyType == other._strategyType &&
           _threshold == other._threshold && _memoryOptimizeThreshold == other._memoryOptimizeThreshold &&
           _limit == other._limit && _diversityConstrain == other._diversityConstrain;
}

} // namespace indexlibv2::config
