#ifndef __INDEXLIB_DEFAULT_PRIMARY_KEY_LOAD_STRATEGY_H
#define __INDEXLIB_DEFAULT_PRIMARY_KEY_LOAD_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index/normal/primarykey/primary_key_load_strategy.h"

IE_NAMESPACE_BEGIN(index);

class DefaultPrimaryKeyLoadStrategy : public PrimaryKeyLoadStrategy
{
public:
    DefaultPrimaryKeyLoadStrategy(const config::PrimaryKeyIndexConfigPtr& pkIndexConfig)
        : mPkConfig(pkIndexConfig)
    {}
    ~DefaultPrimaryKeyLoadStrategy();

public:
    void CreatePrimaryKeyLoadPlans(
            const index_base::PartitionDataPtr& partitionData,
            PrimaryKeyLoadPlanVector& plans) override;

private:
    config::PrimaryKeyIndexConfigPtr mPkConfig;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PrimaryKeyLoadStrategy);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DEFAULT_PRIMARY_KEY_LOAD_STRATEGY_H
