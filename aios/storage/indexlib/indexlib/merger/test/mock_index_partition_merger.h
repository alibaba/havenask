#ifndef __INDEXLIB_MOCK_INDEX_PARTITION_MERGER_H
#define __INDEXLIB_MOCK_INDEX_PARTITION_MERGER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace merger {

class MockIndexPartitionMerger : public IndexPartitionMerger
{
public:
    MockIndexPartitionMerger(const SegmentDirectoryPtr& segDir, const config::IndexPartitionSchemaPtr& schema,
                             const config::IndexPartitionOptions& options,
                             const DumpStrategyPtr& dumpStrategy = DumpStrategyPtr())
        : IndexPartitionMerger(segDir, schema, options, dumpStrategy, NULL, plugin::PluginManagerPtr(),
                               index_base::CommonBranchHinterOption::Test(), PartitionRange())
    {
    }

public:
    MOCK_METHOD(MergeTask, CreateMergeTaskByStrategy,
                (bool optimize, const config::MergeConfig& mergeConfig,
                 const index_base::SegmentMergeInfos& segMergeInfos, const indexlibv2::framework::LevelInfo& levelInfo),
                (const, override));

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MockIndexPartitionMerger);
}} // namespace indexlib::merger

#endif //__INDEXLIB_MOCK_INDEX_PARTITION_MERGER_H
