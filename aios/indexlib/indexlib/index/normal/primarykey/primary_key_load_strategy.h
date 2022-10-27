#ifndef __INDEXLIB_PRIMARY_KEY_LOAD_STRATEGY_H
#define __INDEXLIB_PRIMARY_KEY_LOAD_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index/normal/primarykey/primary_key_load_plan.h"

IE_NAMESPACE_BEGIN(index);

class PrimaryKeyLoadStrategy
{
public:
    PrimaryKeyLoadStrategy()
    {}
    virtual ~PrimaryKeyLoadStrategy() {}

public:
    virtual void CreatePrimaryKeyLoadPlans(
            const index_base::PartitionDataPtr& partitionData,
            PrimaryKeyLoadPlanVector& plans) = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PrimaryKeyLoadStrategy);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_LOAD_STRATEGY_H
