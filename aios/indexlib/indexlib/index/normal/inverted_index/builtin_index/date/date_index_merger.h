#ifndef __INDEXLIB_DATE_INDEX_MERGER_H
#define __INDEXLIB_DATE_INDEX_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/builtin_index/date/time_range_info.h"

IE_NAMESPACE_BEGIN(index);

class DateIndexMerger : public TextIndexMerger
{
public:
    DECLARE_INDEX_MERGER_IDENTIFIER(date);
    DECLARE_INDEX_MERGER_CREATOR(DateIndexMerger, it_date);
public:
    void Merge(const index::MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override
    {
        TextIndexMerger::Merge(resource, segMergeInfos, outputSegMergeInfos);
        MergeRangeInfo(resource, segMergeInfos, outputSegMergeInfos);
    }

    virtual void SortByWeightMerge(const index::MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override
    {
        TextIndexMerger::SortByWeightMerge(resource, segMergeInfos, outputSegMergeInfos);
        MergeRangeInfo(resource, segMergeInfos, outputSegMergeInfos);
    }

private:
    void MergeRangeInfo(const index::MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DateIndexMerger);
/////////////////////////////////////////////
void DateIndexMerger::MergeRangeInfo(const index::MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    TimeRangeInfo info;
    index_base::PartitionDataPtr partitionData = mSegmentDirectory->GetPartitionData();
    assert(partitionData);
    for (size_t i = 0; i < segMergeInfos.size(); ++i)
    {
        index_base::SegmentData segData = partitionData->GetSegmentData(segMergeInfos[i].segmentId);
        file_system::DirectoryPtr indexDirectory = segData.GetIndexDirectory(
                mIndexConfig->GetIndexName(), false);
        if (!indexDirectory || !indexDirectory->IsExist(RANGE_INFO_FILE_NAME))
        {
            continue;
        }
        TimeRangeInfo tmp;
        tmp.Load(indexDirectory);
        if (tmp.IsValid())
        {
            info.Update(tmp.GetMinTime());
            info.Update(tmp.GetMaxTime());
        }
    }
    for (size_t i = 0; i < outputSegMergeInfos.size(); ++i)
    {
        const file_system::DirectoryPtr& indexDirectory = outputSegMergeInfos[i].directory;
        const file_system::DirectoryPtr& mergeDir = GetMergeDir(indexDirectory, false);
        std::string timeRangeStr = autil::legacy::ToJsonString(info);
        mergeDir->Store(RANGE_INFO_FILE_NAME, timeRangeStr);
    }

}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DATE_INDEX_MERGER_H
