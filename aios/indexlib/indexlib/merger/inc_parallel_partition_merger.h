#ifndef __INDEXLIB_INC_PARALLEL_PARTITION_MERGER_H
#define __INDEXLIB_INC_PARALLEL_PARTITION_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/sort_description.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/merger/merge_meta.h"

IE_NAMESPACE_BEGIN(merger);

class IncParallelPartitionMerger : public IndexPartitionMerger
{
public:
    IncParallelPartitionMerger(const IndexPartitionMergerPtr& merger,
                               const std::vector<std::string>& srcPaths,
                               const common::SortDescriptions &sortDesc,
                               misc::MetricProviderPtr metricProvider,
                               const plugin::PluginManagerPtr& pluginManager,
                               const PartitionRange& targetRange = PartitionRange());

    ~IncParallelPartitionMerger();
public:
    index_base::Version GetMergedVersion() const override {
        return mMerger->GetMergedVersion();
    }

private:
    bool hasOngongingModifyOperationsChange(
            const index_base::Version& version, const config::IndexPartitionOptions& options);
public:
    // separate mode.
    MergeMetaPtr CreateMergeMeta(bool optimize,
                                 uint32_t instanceCount,
                                 int64_t currentTs) override
    {
        return mMerger->CreateMergeMeta(optimize, instanceCount, currentTs);
    }
    void PrepareMerge(int64_t currentTs) override;
    void DoMerge(bool optimize, const MergeMetaPtr &mergeMeta,
                 uint32_t instanceId) override {
        return mMerger->DoMerge(optimize, mergeMeta, instanceId);
    }

    MergeMetaPtr LoadMergeMeta(const std::string &mergeMetaPath,
                               bool onlyLoadBasicInfo) override
    {
        return mMerger->LoadMergeMeta(mergeMetaPath, onlyLoadBasicInfo);
    }

    void EndMerge(const MergeMetaPtr& mergeMeta,
                  versionid_t alignVersionId = INVALID_VERSION) override;

    std::string GetMergeIndexRoot() const override
    {
        return mMerger->GetMergeIndexRoot();
    }
    std::string GetMergeMetaDir() const override
    {
        return mMerger->GetMergeMetaDir();
    }

    config::IndexPartitionSchemaPtr GetSchema() const override
    {  return mMerger->GetSchema(); }

    const config::IndexPartitionOptions& GetOptions() const override
    { return mMerger->GetOptions(); }
    util::CounterMapPtr GetCounterMap() override { return mMerger->GetCounterMap(); }
private:
    IndexPartitionMergerPtr mMerger;
    std::vector<std::string> mSrcPaths;
    common::SortDescriptions mSortDesc;
    misc::MetricProviderPtr mMetricProvider;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IncParallelPartitionMerger);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_INC_PARALLEL_PARTITION_MERGER_H
