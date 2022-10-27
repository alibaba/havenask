#ifndef __INDEXLIB_MERGE_META_CREATOR_H
#define __INDEXLIB_MERGE_META_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/merger/index_merge_meta.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"

DECLARE_REFERENCE_CLASS(merger, MergeTask);
DECLARE_REFERENCE_CLASS(merger, SegmentDirectory);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, MergeConfig);
DECLARE_REFERENCE_CLASS(merger, DumpStrategy);
DECLARE_REFERENCE_CLASS(index, ReclaimMapCreator);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);

IE_NAMESPACE_BEGIN(merger);

class MergeMetaCreator
{
public:
    MergeMetaCreator();
    virtual ~MergeMetaCreator();
public:
    virtual void Init(const merger::SegmentDirectoryPtr& segmentDirectory,
                      const config::MergeConfig& mergeConfig,
                      const merger::DumpStrategyPtr& dumpStrategy,
                      const plugin::PluginManagerPtr& pluginManager,
                      uint32_t instanceCount) = 0;
    virtual IndexMergeMeta* Create(MergeTask& task) = 0;
    
    static index_base::SegmentMergeInfos CreateSegmentMergeInfos(
            const config::IndexPartitionSchemaPtr& schema,
            const config::MergeConfig& mergeConfig,
            const merger::SegmentDirectoryPtr& segDir);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeMetaCreator);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGE_META_CREATOR_H
