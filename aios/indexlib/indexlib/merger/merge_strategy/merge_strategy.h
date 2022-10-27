#ifndef __INDEXLIB_MERGE_STRATEGY_H
#define __INDEXLIB_MERGE_STRATEGY_H

#include <tr1/memory>
#include <string>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/merge_strategy_parameter.h"
#include "indexlib/merger/merge_task.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/index_base/index_meta/level_info.h"

IE_NAMESPACE_BEGIN(merger);

class MergeStrategy
{
public:
    MergeStrategy(const merger::SegmentDirectoryPtr &segDir,
                  const config::IndexPartitionSchemaPtr &schema)
        : mSegDir(segDir)
        , mSchema(schema)
    {}
    
    virtual ~MergeStrategy() {}

public:
    virtual std::string GetIdentifier() const = 0;
    virtual void SetParameter(const config::MergeStrategyParameter& param) = 0;
    virtual std::string GetParameter() const = 0;
    virtual MergeTask CreateMergeTask(
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::LevelInfo& levelInfo) = 0;
    virtual MergeTask CreateMergeTaskForOptimize(
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::LevelInfo& levelInfo) = 0;

protected:
    SegmentDirectoryPtr mSegDir;
    config::IndexPartitionSchemaPtr mSchema;
};

DEFINE_SHARED_PTR(MergeStrategy);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGE_STRATEGY_H
