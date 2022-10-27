#include <fslib/fslib.h>
#include "indexlib/config/summary_schema.h"
#include "indexlib/index/normal/summary/local_disk_summary_merger.h"
#include "indexlib/index/normal/summary/common_disk_summary_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/document_merge_info_heap.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/index/segment_directory_base.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/index_meta/parallel_merge_item.h"
#include "indexlib/index_base/merge_task_resource_manager.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/directory_creator.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, LocalDiskSummaryMerger);

LocalDiskSummaryMerger::LocalDiskSummaryMerger(
        const config::SummaryGroupConfigPtr& summaryGroupConfig,
        const index_base::MergeItemHint& hint,
        const index_base::MergeTaskResourceVector& taskResources)
    : mSummaryGroupConfig(summaryGroupConfig)
    , mDataBuf(NULL)
    , mMergeHint(hint)
    , mTaskResources(taskResources)
{
    assert(mSummaryGroupConfig);

    mDataBuf = new char[DATA_BUFFER_SIZE];
    {
        PROCESS_PROFILE_COUNT(memory, LocalDiskSummaryMerger, 
                              DATA_BUFFER_SIZE);
    }
}

LocalDiskSummaryMerger::~LocalDiskSummaryMerger()
{
    delete [] mDataBuf;
    mDataBuf = NULL;
    {
        PROCESS_PROFILE_COUNT(memory, LocalDiskSummaryMerger, 
                          -DATA_BUFFER_SIZE);
    }
}

void LocalDiskSummaryMerger::BeginMerge(const SegmentDirectoryBasePtr& segDir)
{
    mSegmentDirectory = segDir;
}

void LocalDiskSummaryMerger::Merge(const MergerResource& resource,
    const SegmentMergeInfos& segMergeInfos, const OutputSegmentMergeInfos& outputSegMergeInfos)
{
    PrepareOutputDatas(resource, outputSegMergeInfos);
    const ReclaimMapPtr& reclaimMap = resource.reclaimMap;
    for (size_t i = 0, size = segMergeInfos.size(); i < size; ++i)
    {
        if (segMergeInfos[i].segmentInfo.docCount == 0)
        {
            continue;
        }
        docid_t baseDocId = segMergeInfos[i].baseDocId;
        IE_LOG(INFO, "Merge segment %d.", segMergeInfos[i].segmentId);
        SummarySegmentReaderPtr segReader = CreateSummarySegmentReader(segMergeInfos[i]);

        for (docid_t localDocId = 0;
             localDocId < (docid_t)segMergeInfos[i].segmentInfo.docCount; ++localDocId)
        {
            docid_t oldDocId = baseDocId + localDocId;
            docid_t newId = reclaimMap->GetNewId(oldDocId);
            if (newId != INVALID_DOCID)
            {
                auto output = mSegOutputMapper.GetOutput(newId);
                if (output)
                {
                    MergeOneDoc(segReader, localDocId, *output);
                }
            }
        }
        IE_LOG(INFO, "Finish merge segment %d.", segMergeInfos[i].segmentId);
    }
    
    CloseOutputDatas();
}

void LocalDiskSummaryMerger::SortByWeightMerge(const MergerResource& resource,
    const SegmentMergeInfos& segMergeInfos, const OutputSegmentMergeInfos& outputSegMergeInfos)
{
    PrepareOutputDatas(resource, outputSegMergeInfos);
    const ReclaimMapPtr& reclaimMap = resource.reclaimMap;
    
    vector<SummarySegmentReaderPtr> segReaders;
    SegmentMergeInfos validSegMergeInfos;
    for (size_t i = 0; i < segMergeInfos.size(); ++i)
    {
        if (segMergeInfos[i].segmentInfo.docCount == 0)
        {
            continue;
        }
        SummarySegmentReaderPtr segReader = CreateSummarySegmentReader(segMergeInfos[i]);
        segReaders.push_back(segReader);
        validSegMergeInfos.push_back(segMergeInfos[i]);
    }

    DocumentMergeInfoHeap docHeap;
    docHeap.Init(validSegMergeInfos, reclaimMap);
    
    DocumentMergeInfo docInfo;
    while (docHeap.GetNext(docInfo))
    {
        auto output = mSegOutputMapper.GetOutput(docInfo.newDocId);
        if (output)
        {
            MergeOneDoc(segReaders[docInfo.segmentIndex], docInfo.oldDocId, *output);
        }
    }

    CloseOutputDatas();
}

void LocalDiskSummaryMerger::MergeOneDoc(SummarySegmentReaderPtr& segReader, docid_t localDocId, OutputData& output)
{
    uint64_t offset = (uint64_t)output.mergeDataFile->GetLength();
    output.mergeOffsetFile->Write(&offset, sizeof(offset));

    size_t valueLen = segReader->GetRawDataLength(localDocId);

    if (valueLen <= DATA_BUFFER_SIZE)
    {
        if (valueLen > 0)
        {
            segReader->GetRawData(localDocId, mDataBuf, valueLen);
            output.mergeDataFile->Write((void*)mDataBuf, valueLen);            
        }
    }
    else
    {
        char* data = new char[valueLen];
        PROCESS_PROFILE_COUNT(memory, LocalDiskSummaryMerger, valueLen);
        segReader->GetRawData(localDocId, data, valueLen);
        output.mergeDataFile->Write((void*)data, valueLen);
        delete[] data;
        {
            PROCESS_PROFILE_COUNT(memory, LocalDiskSummaryMerger, -valueLen);
        }
    }
}

void LocalDiskSummaryMerger::PrepareOutputDatas(
    const MergerResource& resource, const OutputSegmentMergeInfos& outputSegMergeInfos)
{
    auto createFunc = [this](const index_base::OutputSegmentMergeInfo& outputInfo,
                             size_t outputIdx) {
        file_system::DirectoryPtr realDirectory = outputInfo.directory;
        if (!mSummaryGroupConfig->IsDefaultGroup())
        {
            const string& groupName = mSummaryGroupConfig->GetGroupName();
            realDirectory = realDirectory->MakeDirectory(groupName);
        }
        OutputData output;
        realDirectory->RemoveFile(SUMMARY_OFFSET_FILE_NAME, true);
        realDirectory->RemoveFile(SUMMARY_DATA_FILE_NAME, true);
        output.mergeOffsetFile = realDirectory->CreateFileWriter(SUMMARY_OFFSET_FILE_NAME);
        output.mergeDataFile = realDirectory->CreateFileWriter(SUMMARY_DATA_FILE_NAME);
        return output;
    };
    mSegOutputMapper.Init(resource.reclaimMap, outputSegMergeInfos, createFunc);
}

void LocalDiskSummaryMerger::CloseOutputDatas() {
    for (auto& output : mSegOutputMapper.GetOutputs())
    {
        output.mergeOffsetFile->Close();
        output.mergeDataFile->Close();
    }
}

SummarySegmentReaderPtr LocalDiskSummaryMerger::CreateSummarySegmentReader(
        const SegmentMergeInfo& segMergeInfo)
{
    CommonDiskSummarySegmentReaderPtr summaryReader(
            new CommonDiskSummarySegmentReader(mSummaryGroupConfig));
    const index_base::PartitionDataPtr& partitionData = 
        mSegmentDirectory->GetPartitionData(); 
    assert(partitionData);
    index_base::SegmentData segmentData = partitionData->GetSegmentData(
            segMergeInfo.segmentId);
    summaryReader->Open(segmentData);
    return summaryReader;
}

int64_t LocalDiskSummaryMerger::EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir,
    const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, bool isSortedMerge) const
{
    int64_t size = 0;
    size += sizeof(*this);
    size += DATA_BUFFER_SIZE;

    int64_t segmentReaderMaxMemUse = 0;
    int64_t segmentReaderTotalMemUse = 0;
    for (size_t i = 0; i < segMergeInfos.size(); i++)
    {
        uint32_t docCount = segMergeInfos[i].segmentInfo.docCount;
        if (docCount == 0)
        {
            continue;
        }
        int64_t memUse = CommonDiskSummarySegmentReader::EstimateMemoryUse(docCount);
        segmentReaderTotalMemUse += memUse;
        segmentReaderMaxMemUse = std::max(segmentReaderMaxMemUse, memUse);
    }

    if (isSortedMerge)
    {
        size += segmentReaderTotalMemUse;
    }
    else
    {
        size += segmentReaderMaxMemUse;
    }
    return size;
}

vector<ParallelMergeItem> LocalDiskSummaryMerger::CreateParallelMergeItems(
    const SegmentDirectoryBasePtr& segDir,
    const index_base::SegmentMergeInfos& inPlanSegMergeInfos, uint32_t instanceCount,
    bool isEntireDataSet, index_base::MergeTaskResourceManagerPtr& resMgr) const
{
    vector<ParallelMergeItem> emptyItems;
    return emptyItems;
}

void LocalDiskSummaryMerger::EndParallelMerge(
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, int32_t totalParallelCount,
    const vector<index_base::MergeTaskResourceVector>& instResourceVec)
{
    assert(false);
    INDEXLIB_FATAL_ERROR(UnSupported, "summary not support parallel task merge!");
}

IE_NAMESPACE_END(index);

