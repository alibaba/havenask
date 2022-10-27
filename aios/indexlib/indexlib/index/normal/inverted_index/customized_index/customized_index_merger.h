#ifndef __INDEXLIB_CUSTOMIZED_INDEX_MERGER_H
#define __INDEXLIB_CUSTOMIZED_INDEX_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger_creator.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_reducer.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);

IE_NAMESPACE_BEGIN(index);

class CustomizedIndexMerger : public index::IndexMerger
{
public:
    CustomizedIndexMerger(const plugin::PluginManagerPtr& pluginManager);
    ~CustomizedIndexMerger();

    DECLARE_INDEX_MERGER_IDENTIFIER(customized);
public:
    void Merge(const index::MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;


    void SortByWeightMerge(const index::MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;
    
    void EndMerge() override;

    int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir,
                              const MergerResource& resource, 
                              const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                              bool isSortedMerge) override;

    bool EnableMultiOutputSegmentParallel() const override;

    std::vector<index_base::ParallelMergeItem> CreateParallelMergeItems(
        const SegmentDirectoryBasePtr& segDir,
        const index_base::SegmentMergeInfos& inPlanSegMergeInfos, uint32_t instanceCount,
        bool isEntireDataSet, index_base::MergeTaskResourceManagerPtr& resMgr) const override;

    void EndParallelMerge(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
        int32_t totalParallelCount,
        const std::vector<index_base::MergeTaskResourceVector>& instResourceVec) override;

protected:
    void Init(const config::IndexConfigPtr& indexConfig) override;
    
private:
    void InnerMerge(const index::MergerResource& mergerResource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        index_base::OutputSegmentMergeInfos outputSegMergeInfos, bool isSortMerge);

    void GetFilteredSegments(const SegmentDirectoryBasePtr& segDir,
                             const index_base::SegmentMergeInfos& segMergeInfos,
                             std::vector<file_system::DirectoryPtr> &indexDirs,
                             index_base::SegmentMergeInfos &filteredSegMergeInfos) const;

private:
    OnDiskIndexIteratorCreatorPtr CreateOnDiskIndexIteratorCreator() override;

private:
    plugin::PluginManagerPtr mPluginManager;
    IndexReducerPtr mIndexReducer;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomizedIndexMerger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_CUSTOMIZED_INDEX_MERGER_H
