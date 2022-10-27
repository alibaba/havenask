#ifndef __INDEXLIB_TRUNCATE_INDEX_PROPERTY_H
#define __INDEXLIB_TRUNCATE_INDEX_PROPERTY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/sort_param.h"
#include "indexlib/config/truncate_strategy.h"
#include "indexlib/config/truncate_profile.h"

IE_NAMESPACE_BEGIN(config);

class TruncateIndexProperty
{
public:
    TruncateIndexProperty();
    ~TruncateIndexProperty();

public:
    bool HasFilter() const
    {
        return mTruncateStrategy->GetDiversityConstrain().NeedFilter();
    }

    bool IsFilterByMeta() const
    {
        return mTruncateStrategy->GetDiversityConstrain().IsFilterByMeta();
    }

    bool IsFilterByTimeStamp() const
    {
        return mTruncateStrategy->GetDiversityConstrain().IsFilterByTimeStamp();
    }

    bool HasDistinct() const
    {
        return mTruncateStrategy->GetDiversityConstrain().NeedDistinct();
    }
    bool HasSort() const { return mTruncateProfile->HasSort(); }

    size_t GetSortDimenNum() const
    { return mTruncateProfile->GetSortDimenNum(); }

    std::string GetStrategyType() const 
    { 
        return mTruncateStrategy->GetStrategyType();
    }

    uint64_t GetThreshold() const 
    {
        return mTruncateStrategy->GetThreshold();
    }

public:
    std::string mTruncateIndexName;
    TruncateStrategyPtr mTruncateStrategy;
    TruncateProfilePtr mTruncateProfile;

private:
    IE_LOG_DECLARE();
};

typedef std::vector<TruncateIndexProperty> TruncateIndexPropertyVector;

DEFINE_SHARED_PTR(TruncateIndexProperty);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_TRUNCATE_INDEX_PROPERTY_H
