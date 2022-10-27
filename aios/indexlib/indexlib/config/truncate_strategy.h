#ifndef __INDEXLIB_TRUNCATE_STRATEGY_CONFIG_H
#define __INDEXLIB_TRUNCATE_STRATEGY_CONFIG_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/diversity_constrain.h"
#include "indexlib/config/config_define.h"
#include "indexlib/misc/exception.h"

IE_NAMESPACE_BEGIN(config);

class TruncateStrategy : public autil::legacy::Jsonizable
{
public:
    TruncateStrategy();
    ~TruncateStrategy();
    
public:
    const std::string& GetStrategyName() const { return mStrategyName; }
    const std::string& GetStrategyType() const { return mStrategyType; }
    std::vector<std::string>& GetProfileNames() { return mTruncateProfileNames; }
    uint64_t GetThreshold() const { return mThreshold; }
    int32_t GetMemoryOptimizeThreshold() const 
    { return mMemoryOptimizeThreshold; }
    uint64_t GetLimit() const { return mLimit; }
    const DiversityConstrain& GetDiversityConstrain() const 
    {
        return mDiversityConstrain;
    }

    DiversityConstrain& GetDiversityConstrain()  
    {
        return mDiversityConstrain;
    }

    void SetStrategyName(const std::string& strategyName)
    {
        mStrategyName = strategyName;
    }

    void SetStrategyType(const std::string& strategyType)
    {
        mStrategyType = strategyType;
    }
    void SetThreshold(uint64_t threshold) { mThreshold = threshold; }
    void SetMemoryOptimizeThreshold(int32_t threshold)
    { mMemoryOptimizeThreshold = threshold; }
    void SetLimit(uint64_t limit) { mLimit = limit; }
    void SetDiversityConstrain(const DiversityConstrain& divCons)
    {
        mDiversityConstrain = divCons;
    }
    // for d2 compitable
    void AddProfileName(const std::string& profileName)
    {
        mTruncateProfileNames.push_back(profileName);
    }

    bool HasLimit() const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    void Check() const;
    bool operator==(const TruncateStrategy& other) const;

private:
    static const uint64_t MAX_TRUNC_THRESHOLD;
    static const uint64_t MAX_TRUNC_LIMIT;
    static const int32_t MAX_MEMORY_OPTIMIZE_PERCENTAGE;
    static const int32_t DEFAULT_MEMORY_OPTIMIZE_PERCENTAGE;

private:
    std::string mStrategyName;
    std::string mStrategyType;
    std::vector<std::string> mTruncateProfileNames;
    uint64_t mThreshold;
    int32_t mMemoryOptimizeThreshold;
    uint64_t mLimit;
    DiversityConstrain mDiversityConstrain;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateStrategy);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_TRUNCATE_STRATEGY_CONFIG_H
