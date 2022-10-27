#ifndef __INDEXLIB_COMBINE_SEGMENTS_PRIMARY_KEY_LOAD_STRATEGY_H
#define __INDEXLIB_COMBINE_SEGMENTS_PRIMARY_KEY_LOAD_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/primary_key_load_strategy.h"
#include "indexlib/config/primary_key_load_strategy_param.h"

IE_NAMESPACE_BEGIN(index);

class CombineSegmentsPrimaryKeyLoadStrategy : public PrimaryKeyLoadStrategy
{
public:
    CombineSegmentsPrimaryKeyLoadStrategy(
            const config::PrimaryKeyIndexConfigPtr& pkIndexConfig)
        : mPkConfig(pkIndexConfig)
    {
        config::PrimaryKeyLoadStrategyParamPtr param = 
            mPkConfig->GetPKLoadStrategyParam();
        assert(param);
        assert(param->NeedCombineSegments());
        mMaxDocCount = param->GetMaxDocCount();
    }
    ~CombineSegmentsPrimaryKeyLoadStrategy() {}
public:
    void CreatePrimaryKeyLoadPlans(
            const index_base::PartitionDataPtr& partitionData,
            PrimaryKeyLoadPlanVector& plans) override;

private:
    bool NeedCreateNewPlan(bool isRtSegment, const PrimaryKeyLoadPlanPtr& plan,
                           bool isRtPlan);
    void PushBackPlan(const PrimaryKeyLoadPlanPtr& plan, 
                      PrimaryKeyLoadPlanVector& plans);
    size_t GetDeleteCount(const index_base::SegmentData& segData,
                          const DeletionMapReaderPtr& deletionMapReader);
private:
    config::PrimaryKeyIndexConfigPtr mPkConfig;
    size_t mMaxDocCount;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CombineSegmentsPrimaryKeyLoadStrategy);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_COMBINE_SEGMENTS_PRIMARY_KEY_LOAD_STRATEGY_H
