#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger.h"
#include "indexlib/index/normal/deletionmap/deletion_map_merger.h"
#include "indexlib/index/normal/framework/index_merger.h"
#include "indexlib/index/normal/summary/local_disk_summary_merger.h"
#include "indexlib/index_base/index_meta/parallel_merge_item.h"
#include "indexlib/index_base/merge_task_resource_manager.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/merge_file_system.h"
#include "indexlib/merger/merge_work_item_creator.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);

namespace indexlib { namespace merger {

class FakeAttributeMerger : public index::AttributeMerger
{
public:
    DECLARE_INDEX_MERGER_IDENTIFIER(FakeAttributeMerger);
    void Merge(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
               const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override
    {
    }

    void SortByWeightMerge(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                           const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override
    {
    }

    int64_t EstimateMemoryUse(const index::SegmentDirectoryBasePtr& segDir, const index::MergerResource& resource,
                              const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                              bool isSortedMerge) const override
    {
        return 0;
    }

    std::vector<index_base::ParallelMergeItem>
    CreateParallelMergeItems(const index::SegmentDirectoryBasePtr& segDir,
                             const index_base::SegmentMergeInfos& inPlanSegMergeInfos, uint32_t instanceCount,
                             bool isEntireDataSet, index_base::MergeTaskResourceManagerPtr& resMgr) const override
    {
        return parallelMergeItems;
    }

public:
    std::vector<index_base::ParallelMergeItem> parallelMergeItems;
};

class FakeIndexMerger : public index::IndexMerger
{
public:
    DECLARE_INDEX_MERGER_IDENTIFIER(FakeIndexMerger);

    void Merge(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
               const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override
    {
        assert(outputSegMergeInfos.size() > 0);
        mRoot = outputSegMergeInfos[0].path;
        mSegMergeInfos = segMergeInfos;
        mReclaimMap = resource.reclaimMap;
    }

    void SortByWeightMerge(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                           const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override
    {
        Merge(resource, segMergeInfos, outputSegMergeInfos);
    }

    int64_t EstimateMemoryUse(const index::SegmentDirectoryBasePtr& segDir, const index::MergerResource& resource,
                              const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                              bool isSortedMerge) const override
    {
        return 0;
    }

    std::vector<index_base::ParallelMergeItem>
    CreateParallelMergeItems(const index::SegmentDirectoryBasePtr& segDir,
                             const index_base::SegmentMergeInfos& inPlanSegMergeInfos, uint32_t instanceCount,
                             bool isEntireDataSet, index_base::MergeTaskResourceManagerPtr& resMgr) const override
    {
        return parallelMergeItems;
    }

public:
    std::string mRoot;
    index_base::SegmentMergeInfos mSegMergeInfos;
    index::ReclaimMapPtr mReclaimMap;

public:
    std::vector<index_base::ParallelMergeItem> parallelMergeItems;
};

class FakeSummaryMerger : public index::SummaryMerger
{
public:
    FakeSummaryMerger() : SummaryMerger(config::SummaryGroupConfigPtr()) {}
    void BeginMerge(const index::SegmentDirectoryBasePtr& segDir) override {}
    void Merge(const std::string& dir, const index_base::SegmentMergeInfos& segMergeInfos,
               const index::ReclaimMapPtr& reclaimMap)
    {
    }
    void SortByWeightMerge(const std::string& dir, const index_base::SegmentMergeInfos& segMergeInfos,
                           const index::ReclaimMapPtr& reclaimMap)
    {
    }

    void Merge(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
               const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override
    {
    }

    void SortByWeightMerge(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                           const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override
    {
    }

    int64_t EstimateMemoryUse(const index::SegmentDirectoryBasePtr& segDir, const index::MergerResource& resource,
                              const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                              bool isSortedMerge) const override
    {
        return 0;
    }

    std::vector<index_base::ParallelMergeItem>
    CreateParallelMergeItems(const index::SegmentDirectoryBasePtr& segDir,
                             const index_base::SegmentMergeInfos& inPlanSegMergeInfos, uint32_t instanceCount,
                             bool isEntireDataSet, index_base::MergeTaskResourceManagerPtr& resMgr) const override
    {
        return parallelMergeItems;
    }

    void EndParallelMerge(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, int32_t totalParallelCount,
                          const std::vector<index_base::MergeTaskResourceVector>& instResourceVec) override
    {
    }

public:
    std::vector<index_base::ParallelMergeItem> parallelMergeItems;

private:
    virtual index::VarLenDataParam CreateVarLenDataParam() const override { return index::VarLenDataParam(); }

    virtual file_system::DirectoryPtr CreateInputDirectory(const index_base::SegmentData& segmentData) const override
    {
        return file_system::DirectoryPtr();
    }

    virtual file_system::DirectoryPtr
    CreateOutputDirectory(const index_base::OutputSegmentMergeInfo& outputSegMergeInfo) const override
    {
        return file_system::DirectoryPtr();
    }
};

class FakeSourceGroupMerger : public index::SourceGroupMerger
{
public:
    FakeSourceGroupMerger(config::SourceGroupConfigPtr groupConfig) : index::SourceGroupMerger(groupConfig) {}
    void BeginMerge(const index::SegmentDirectoryBasePtr& segDir) override {}
    void Merge(const std::string& dir, const index_base::SegmentMergeInfos& segMergeInfos,
               const index::ReclaimMapPtr& reclaimMap)
    {
    }
    void SortByWeightMerge(const std::string& dir, const index_base::SegmentMergeInfos& segMergeInfos,
                           const index::ReclaimMapPtr& reclaimMap)
    {
    }

    void Merge(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
               const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override
    {
    }

    void SortByWeightMerge(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                           const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override
    {
    }

    int64_t EstimateMemoryUse(const index::SegmentDirectoryBasePtr& segDir, const index::MergerResource& resource,
                              const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                              bool isSortedMerge) const override
    {
        return 0;
    }

    std::vector<index_base::ParallelMergeItem>
    CreateParallelMergeItems(const index::SegmentDirectoryBasePtr& segDir,
                             const index_base::SegmentMergeInfos& inPlanSegMergeInfos, uint32_t instanceCount,
                             bool isEntireDataSet, index_base::MergeTaskResourceManagerPtr& resMgr) const override
    {
        return parallelMergeItems;
    }

    void EndParallelMerge(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, int32_t totalParallelCount,
                          const std::vector<index_base::MergeTaskResourceVector>& instResourceVec) override
    {
    }

public:
    std::vector<index_base::ParallelMergeItem> parallelMergeItems;
};

class FakeDeletionMapMerger : public index::DeletionMapMerger
{
    void Merge(const std::string& dir, const index_base::SegmentMergeInfos& segMergeInfos,
               const index::ReclaimMapPtr& reclaimMap)
    {
    }
};

class MergeWorkItemCreatorMock : public MergeWorkItemCreator
{
public:
    MergeWorkItemCreatorMock(const config::IndexPartitionSchemaPtr& schema, const config::MergeConfig& mergeConfig,
                             const IndexMergeMeta* mergeMeta, const merger::SegmentDirectoryPtr& segmentDirectory,
                             const merger::SegmentDirectoryPtr& subSegmentDirectory, bool isSortedMerge,
                             const config::IndexPartitionOptions& options, const MergeFileSystemPtr& mergeFileSystem)
        : MergeWorkItemCreator(schema, mergeConfig, mergeMeta, segmentDirectory, subSegmentDirectory, isSortedMerge,
                               false, NULL, util::CounterMapPtr(), options, mergeFileSystem, plugin::PluginManagerPtr())
    {
    }
    ~MergeWorkItemCreatorMock() {}

public:
    index::IndexMergerPtr
    CreateIndexMerger(const MergeTaskItem& item, const config::IndexPartitionSchemaPtr& schema,
                      index::legacy::TruncateIndexWriterCreator* truncateCreator,
                      index::legacy::AdaptiveBitmapIndexWriterCreator* adaptiveCreator) const override
    {
        return index::IndexMergerPtr(new FakeIndexMerger());
    }

    index::AttributeMergerPtr CreateAttributeMerger(const MergeTaskItem& item,
                                                    const config::IndexPartitionSchemaPtr& schema) const override
    {
        return index::AttributeMergerPtr(new FakeAttributeMerger());
    }

    index::SummaryMergerPtr CreateSummaryMerger(const MergeTaskItem& item,
                                                const config::IndexPartitionSchemaPtr& schema) const override
    {
        return index::SummaryMergerPtr(new FakeSummaryMerger());
    }

    index::SourceGroupMergerPtr CreateSourceGroupMerger(const MergeTaskItem& item,
                                                        const config::IndexPartitionSchemaPtr& schema) const override
    {
        config::SourceGroupConfigPtr groupConfig(new config::SourceGroupConfig);
        groupConfig->SetGroupId(0);
        return index::SourceGroupMergerPtr(new FakeSourceGroupMerger(groupConfig));
    }

    index::DeletionMapMergerPtr CreateDeletionMapMerger() const override
    {
        return index::DeletionMapMergerPtr(new FakeDeletionMapMerger());
    }
};

DEFINE_SHARED_PTR(MergeWorkItemCreatorMock);
}} // namespace indexlib::merger
