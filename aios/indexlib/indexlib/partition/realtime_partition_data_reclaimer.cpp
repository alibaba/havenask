#include <autil/StringUtil.h>
#include "indexlib/partition/realtime_partition_data_reclaimer.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/segment/normal_segment_dump_item.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader_factory.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/key_iterator.h"
#include "indexlib/index/calculator/partition_size_calculator.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, RealtimePartitionDataReclaimer);

RealtimePartitionDataReclaimer::RealtimePartitionDataReclaimer(
    const IndexPartitionSchemaPtr& schema,
    const config::IndexPartitionOptions& options)
    : mSchema(schema)
    , mOptions(options)
{
}

RealtimePartitionDataReclaimer::~RealtimePartitionDataReclaimer() 
{
}

void RealtimePartitionDataReclaimer::Reclaim(int64_t timestamp,
        const PartitionDataPtr& buildingPartData)
{
    IE_LOG(INFO, "reclaim begin");
    TrimObsoleteAndEmptyRtSegments(timestamp, buildingPartData);
    RemoveObsoleteRtDocs(timestamp, buildingPartData);
    TrimObsoleteAndEmptyRtSegments(timestamp, buildingPartData);
    IE_LOG(INFO, "reclaim end");
}

void RealtimePartitionDataReclaimer::ExtractSegmentsToReclaimFromPartitionData(
        const PartitionDataPtr& partData, int64_t reclaimTimestamp,
        std::vector<segmentid_t>& segIdsToReclaim)
{
    SimpleSegmentMetas segMetas;
    ExtractRtSegMetasFromPartitionData(partData, segMetas);
    DeletionMapReaderPtr delMapReader = partData->GetDeletionMapReader();
    ExtractSegmentsToReclaim(segMetas, reclaimTimestamp, delMapReader, segIdsToReclaim);
}

void RealtimePartitionDataReclaimer::RemoveObsoleteRtDocs(
        int64_t reclaimTimestamp, const PartitionDataPtr& partitionData)
{
    IE_LOG(INFO, "RemoveObsoleteRtDocs begin");
    // TODO: remove args options and calculator
    IndexConfigPtr indexConfig = mSchema->GetIndexSchema()->GetIndexConfig(
            VIRTUAL_TIMESTAMP_INDEX_NAME);
    IndexReaderPtr indexReader(IndexReaderFactory::CreateIndexReader(
                    indexConfig->GetIndexType()));
    indexReader->Open(indexConfig, partitionData);
    assert(indexReader);
    KeyIteratorPtr keyIter = indexReader->CreateKeyIterator("");

    PartitionModifierPtr modifier(
            PartitionModifierCreator::CreatePatchModifier(
                    mSchema, partitionData, false, false));
    assert(modifier);

    while (keyIter->HasNext())
    {
        string strTs;
        PostingIteratorPtr postIter(keyIter->NextPosting(strTs));
        assert(postIter);

        int64_t ts;
        if (!StringUtil::strToInt64(strTs.c_str(), ts))
        {
            continue;
        }
        if (ts >= reclaimTimestamp)
        {
            break;
        }
        docid_t docId = INVALID_DOCID;
        while ((docId = postIter->SeekDoc(docId)) != INVALID_DOCID)
        {
            modifier->RemoveDocument(docId);
        }
    }
    if (!modifier->IsDirty())
    {
        return;
    }

    InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
    NormalSegmentDumpItemPtr segmentDumpItem(new NormalSegmentDumpItem(mOptions, mSchema, ""));
    segmentDumpItem->Init(NULL, partitionData, modifier, FlushedLocatorContainerPtr(), true);
    segmentDumpItem->Dump();
    partitionData->AddBuiltSegment(
        inMemSegment->GetSegmentId(), inMemSegment->GetSegmentInfo());
    partitionData->CommitVersion();
    inMemSegment->SetStatus(InMemorySegment::DUMPED);
    partitionData->CreateNewSegment();
    IE_LOG(INFO, "RemoveObsoleteRtDocs end");
}

void RealtimePartitionDataReclaimer::TrimObsoleteAndEmptyRtSegments(
        int64_t reclaimTimestamp, const PartitionDataPtr& partData)
{
    IE_LOG(INFO, "trim begin");
    SimpleSegmentMetas segMetas;
    ExtractRtSegMetasFromPartitionData(partData, segMetas);
    
    DeletionMapReaderPtr delMapReader = partData->GetDeletionMapReader();
    vector<segmentid_t> segIdsToRemove;
    ExtractSegmentsToReclaim(segMetas, reclaimTimestamp, delMapReader, segIdsToRemove);

    RemoveSegmentIds(partData, segIdsToRemove);
    IE_LOG(INFO, "trim end");
}

void RealtimePartitionDataReclaimer::TrimBuildingSegment(
        int64_t reclaimTimestamp, const PartitionDataPtr& partData)
{
    IE_LOG(INFO, "trim building segment begin");

    int64_t buildingTs = INVALID_TIMESTAMP;
    const InMemorySegmentPtr& inMemorySegment = partData->GetInMemorySegment();
    if (inMemorySegment)
    {
        const SegmentInfoPtr& segmentInfo = inMemorySegment->GetSegmentInfo();
        if (segmentInfo)
        {
            buildingTs = segmentInfo->timestamp;
        }
        else
        {
            assert(false);
            IE_LOG(ERROR, "segmentInfo in inMemorySegment[%s]",
                   inMemorySegment->GetDirectory()->GetPath().c_str());
        }
    }
    if ((buildingTs == INVALID_TIMESTAMP) || (buildingTs < reclaimTimestamp))
    {
        const SegmentDirectoryPtr& segDir = partData->GetSegmentDirectory();
        segDir->IncLastSegmentId();
        partData->CommitVersion();
        partData->ResetInMemorySegment();
        IE_LOG(INFO, "building segment droped");
    }

    IE_LOG(INFO, "trim building segment end");
}


void RealtimePartitionDataReclaimer::ExtractRtSegMetasFromPartitionData(
        const PartitionDataPtr& partData,
        SimpleSegmentMetas& segMetas)
{
    segMetas.clear();

    PartitionData::Iterator it;
    for (it = partData->Begin(); it != partData->End(); it++)
    {
        const SegmentData& segData = *it;
        if (!RealtimeSegmentDirectory::IsRtSegmentId(segData.GetSegmentId()))
        {
            continue;
        }

        SimpleSegmentMeta meta;
        meta.segmentId = segData.GetSegmentId();
        meta.docCount = segData.GetSegmentInfo().docCount;
        meta.timestamp = segData.GetSegmentInfo().timestamp;
        meta.hasOperation = (segData.GetOperationDirectory(false) != NULL);
        segMetas.push_back(meta);
    }
}

void RealtimePartitionDataReclaimer::ExtractSegmentsToReclaim(
        const SimpleSegmentMetas& segMetas, 
        int64_t reclaimTimestamp,
        const DeletionMapReaderPtr& deletionMap,
        vector<segmentid_t>& outputSegIds)
{
    outputSegIds.clear();
    if (segMetas.empty())
    {
        return;
    }

    for (size_t i = 0; i < segMetas.size(); ++i)
    {
        if (NeedReclaimSegment(segMetas[i], reclaimTimestamp, deletionMap))
        {
            outputSegIds.push_back(segMetas[i].segmentId);
        }
        else
        {
            break;
        }
    }
}

bool RealtimePartitionDataReclaimer::NeedReclaimSegment(
        const SimpleSegmentMeta& segMeta, int64_t reclaimTimestamp, 
        const DeletionMapReaderPtr& deletionMap)
{
    if (segMeta.timestamp < reclaimTimestamp)
    {
        return true;
    }

    if (segMeta.hasOperation)
    {
        return false;
    }
    
    if (deletionMap && 
        deletionMap->GetDeletedDocCount(segMeta.segmentId) == segMeta.docCount)
    {
        return true;
    }
    return false;
}

void RealtimePartitionDataReclaimer::RemoveSegmentIds(
        const PartitionDataPtr& partData,
        const vector<segmentid_t>& segIdsToRemove)
{
    if (segIdsToRemove.empty())
    {
        return;
    }

    partData->RemoveSegments(segIdsToRemove);
    partData->CommitVersion();
    partData->CreateNewSegment();
}


IE_NAMESPACE_END(partition);

