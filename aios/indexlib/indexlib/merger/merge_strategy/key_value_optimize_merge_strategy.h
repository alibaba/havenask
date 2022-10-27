#ifndef __INDEXLIB_KEY_VALUE_OPTIMIZE_MERGE_STRATEGY_H
#define __INDEXLIB_KEY_VALUE_OPTIMIZE_MERGE_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/merger/merge_strategy/merge_strategy.h"

IE_NAMESPACE_BEGIN(merger);

class KeyValueOptimizeMergeStrategy : public MergeStrategy
{
public:
    KeyValueOptimizeMergeStrategy(const merger::SegmentDirectoryPtr& segDir,
                                  const config::IndexPartitionSchemaPtr& schema);

    ~KeyValueOptimizeMergeStrategy();

public:
    std::string GetIdentifier() const override
    { return OPTIMIZE_MERGE_STRATEGY_STR; }

    void SetParameter(const config::MergeStrategyParameter& param) override;
    std::string GetParameter() const override;
    MergeTask CreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::LevelInfo& levelInfo) override;
    MergeTask CreateMergeTaskForOptimize(
            const index_base::SegmentMergeInfos& segMergeInfos,
            const index_base::LevelInfo& levelInfo) override;

private:
    bool NeedMerge(const index_base::SegmentMergeInfos& segMergeInfos,
                   const index_base::LevelInfo& levelInfo) const;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KeyValueOptimizeMergeStrategy);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_KEY_VALUE_OPTIMIZE_MERGE_STRATEGY_H
