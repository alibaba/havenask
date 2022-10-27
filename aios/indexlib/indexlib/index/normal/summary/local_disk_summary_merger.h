#ifndef __INDEXLIB_LOCAL_DISK_SUMMARY_MERGER_H
#define __INDEXLIB_LOCAL_DISK_SUMMARY_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/summary/summary_merger.h"
#include "indexlib/config/summary_config.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_base/index_meta/parallel_merge_item.h"
#include "indexlib/index_base/index_meta/merge_task_resource.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/index/normal/summary/summary_segment_reader.h"
#include "indexlib/index/segment_output_mapper.h"

DECLARE_REFERENCE_CLASS(file_system, FileWriter);
DECLARE_REFERENCE_CLASS(config, SummaryGroupConfig);

IE_NAMESPACE_BEGIN(index);

class LocalDiskSummaryMerger : public SummaryMerger
{
    struct OutputData
    {
        file_system::FileWriterPtr mergeOffsetFile;
        file_system::FileWriterPtr mergeDataFile;
    };
public:
    LocalDiskSummaryMerger(const config::SummaryGroupConfigPtr& summaryGroupConfig,
        const index_base::MergeItemHint& hint = index_base::MergeItemHint(),
        const index_base::MergeTaskResourceVector& taskResources
        = index_base::MergeTaskResourceVector());
    virtual ~LocalDiskSummaryMerger();

public:
    void BeginMerge(const SegmentDirectoryBasePtr& segDir) override;

    virtual void Merge(const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    virtual void SortByWeightMerge(const MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir,
        const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
        bool isSortedMerge) const override;

    std::vector<index_base::ParallelMergeItem> CreateParallelMergeItems(
        const SegmentDirectoryBasePtr& segDir,
        const index_base::SegmentMergeInfos& inPlanSegMergeInfos, uint32_t instanceCount,
        bool isEntireDataSet, index_base::MergeTaskResourceManagerPtr& resMgr) const override;

    void EndParallelMerge(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
        int32_t totalParallelCount,
        const std::vector<index_base::MergeTaskResourceVector>& instResourceVec) override;

private:
    SummarySegmentReaderPtr CreateSummarySegmentReader(const index_base::SegmentMergeInfo& segMergeInfo);
    
    void PrepareOutputDatas(const MergerResource& resource,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);
    void CloseOutputDatas();
    void MergeOneDoc(SummarySegmentReaderPtr& segReader, docid_t localDocId, OutputData& output);

protected:
    SegmentDirectoryBasePtr mSegmentDirectory;
    config::SummaryGroupConfigPtr mSummaryGroupConfig;

private:
    char* mDataBuf;

    index_base::MergeItemHint mMergeHint;
    index_base::MergeTaskResourceVector mTaskResources;

    SegmentOutputMapper<OutputData> mSegOutputMapper;

private:
    static const int DATA_BUFFER_SIZE = 1024 * 1024 * 2;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LocalDiskSummaryMerger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_LOCAL_DISK_SUMMARY_MERGER_H
