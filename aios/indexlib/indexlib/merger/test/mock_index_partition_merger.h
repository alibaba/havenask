#ifndef __INDEXLIB_MOCK_INDEX_PARTITION_MERGER_H
#define __INDEXLIB_MOCK_INDEX_PARTITION_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/merger/index_partition_merger.h"

IE_NAMESPACE_BEGIN(merger);

class MockIndexPartitionMerger : public IndexPartitionMerger
{
public:
    MockIndexPartitionMerger(const SegmentDirectoryPtr& segDir, 
                             const config::IndexPartitionSchemaPtr& schema,
                             const config::IndexPartitionOptions& options,
                             const DumpStrategyPtr &dumpStrategy = DumpStrategyPtr())
        : IndexPartitionMerger(segDir, schema, options, dumpStrategy,
                               NULL, plugin::PluginManagerPtr())
    {}

public:
    MOCK_CONST_METHOD4(CreateMergeTaskByStrategy, MergeTask(
                           bool optimize, const config::MergeConfig& mergeConfig,
                           const index_base::SegmentMergeInfos& segMergeInfos,
                           const index_base::LevelInfo& levelInfo));

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MockIndexPartitionMerger);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MOCK_INDEX_PARTITION_MERGER_H
