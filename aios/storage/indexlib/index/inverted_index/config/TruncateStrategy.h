#pragma once

#include <memory>

#include "autil/Log.h"
#include "indexlib/index/inverted_index/Types.h"
#include "indexlib/index/inverted_index/config/DiversityConstrain.h"

namespace indexlibv2::config {

class TruncateStrategy : public autil::legacy::Jsonizable
{
public:
    TruncateStrategy();
    ~TruncateStrategy() = default;

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool operator==(const TruncateStrategy& other) const;
    void SetStrategyName(const std::string& strategyName) { _strategyName = strategyName; }
    const std::string& GetStrategyName() const { return _strategyName; }
    const std::string& GetStrategyType() const { return _strategyType; }
    std::vector<std::string>& GetProfileNames() { return _truncateProfileNames; }
    uint64_t GetThreshold() const { return _threshold; }
    int32_t GetMemoryOptimizeThreshold() const { return _memoryOptimizeThreshold; }
    uint64_t GetLimit() const { return _limit; }
    const DiversityConstrain& GetDiversityConstrain() const { return _diversityConstrain; }
    DiversityConstrain& GetDiversityConstrain() { return _diversityConstrain; }
    bool HasLimit() const;

    bool Check() const;

private:
    static constexpr uint64_t MAX_TRUNC_LIMIT = std::numeric_limits<indexlib::df_t>::max();
    static constexpr int32_t MAX_MEMORY_OPTIMIZE_PERCENTAGE = 100;
    static constexpr int32_t DEFAULT_MEMORY_OPTIMIZE_PERCENTAGE = 20;

    std::string _strategyName;
    std::string _strategyType;
    std::vector<std::string> _truncateProfileNames;
    uint64_t _threshold;
    int32_t _memoryOptimizeThreshold;
    uint64_t _limit;
    DiversityConstrain _diversityConstrain;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
