#include "indexlib/partition/operation_queue/operation_iterator.h"
#include "indexlib/partition/operation_queue/operation_writer.h"
#include "indexlib/partition/operation_queue/normal_segment_operation_iterator.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/directory.h"

using namespace std;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OperationIterator);

OperationIterator::OperationIterator(const PartitionDataPtr& partitionData,
                                     const IndexPartitionSchemaPtr& schema)
    : mCurrentSegIdx(-1)
    , mTimestamp(INVALID_TIMESTAMP)
    , mPartitionData(partitionData)
    , mSchema(schema)
{}

void OperationIterator::Init(
    int64_t timestamp, const OperationCursor& skipCursor)
{
    mTimestamp = timestamp;
    InitBuiltSegments(mPartitionData, skipCursor);
    InitInMemSegments(mPartitionData, skipCursor);    
    InitSegmentIterator(skipCursor);
}

void OperationIterator::InitSegmentIterator(const OperationCursor& skipCursor)
{
    int segIdx = -1; // points to the segment to be load
    int offset = 0;

    mLastOpCursor = skipCursor;
    if (LocateStartLoadPosition(skipCursor, segIdx, offset))
    {
        mCurrentSegIter = LoadSegIterator(segIdx, offset);
    }
    else
    {
        mCurrentSegIdx = mSegDataVec.size() + mDumpingSegments.size();
        mCurrentSegIter = SegmentOperationIteratorPtr();
    }
}

bool OperationIterator::LocateStartLoadPosition(
    const OperationCursor& skipCursor, int& segIdx, int& offset)
{
    return LocateStartLoadPositionInNormalSegment(skipCursor, segIdx, offset)
        || LocateStartLoadPositionInDumpingSegment(skipCursor, segIdx, offset)
        || LocateStartLoadPositionInBuildingSegment(skipCursor, segIdx, offset);
}

bool OperationIterator::LocateStartLoadPositionInNormalSegment(
    const OperationCursor& skipCursor, int& segIdx, int& offset)
{
    for (size_t i = 0; i < mSegDataVec.size(); i++)
    {
        if (mSegDataVec[i].GetSegmentId() == skipCursor.segId)
        {
            size_t opCount = mSegMetaVec[i].GetOperationCount();
            if (skipCursor.pos >= (int32_t)opCount)
            {
                IE_LOG(ERROR, "invalid skip cursor: [segid:%d, pos:%d], "
                       "totalOpCount [%lu]",
                       skipCursor.segId, skipCursor.pos, opCount);
                return false;
            }
            // if skipCursor points to the last operation of current segment,
            // then set load position to the beginning of the next segment
            if (skipCursor.pos == (int32_t)opCount - 1)
            {
                segIdx = (int)i + 1;
                offset = 0;
            }
            else
            {
                segIdx = (int)i;
                offset = skipCursor.pos + 1;
            }
            return true;
        }

        // if current segment's id greater than skipCursor's 
        // then set load position to the beginning of the current segment
        if (mSegDataVec[i].GetSegmentId() > skipCursor.segId)
        {
            segIdx = (int)i;
            offset = 0;
            return true;
        }
    }
    return false;
}

bool OperationIterator::LocateStartLoadPositionInDumpingSegment(
        const OperationCursor& skipCursor, int& segIdx, int& offset)
{
    int startCursor = (int)mSegDataVec.size();
    for (size_t i = 0; i < mDumpingSegments.size(); i++)
    {
        segmentid_t segId = mDumpingSegments[i]->GetSegmentId();
        OperationWriterPtr opWriter =
            DYNAMIC_POINTER_CAST(OperationWriter, mDumpingSegments[i]->GetOperationWriter());
        if (!opWriter)
        {
            IE_LOG(ERROR, "get operation writer failed");
            return false;
        }
        OperationMeta opMeta = opWriter->GetOperationMeta();
        if (segId == skipCursor.segId)
        {
            size_t opCount = opMeta.GetOperationCount();
            if (skipCursor.pos >= (int32_t)opCount)
            {
                IE_LOG(ERROR, "invalid skip cursor: [segid:%d, pos:%d], "
                       "totalOpCount [%lu]",
                       skipCursor.segId, skipCursor.pos, opCount);
                return false;
            }
            // if skipCursor points to the last operation of current segment,
            // then set load position to the beginning of the next segment
            if (skipCursor.pos == (int32_t)opCount - 1)
            {
                segIdx = (int)i + 1 + startCursor;
                offset = 0;
            }
            else
            {
                segIdx = (int)i + startCursor;
                offset = skipCursor.pos + 1;
            }
            return true;
        }

        // if current segment's id greater than skipCursor's 
        // then set load position to the beginning of the current segment
        if (segId > skipCursor.segId)
        {
            segIdx = (int)i + startCursor;
            offset = 0;
            return true;
        }
    }
    return false;
}

bool OperationIterator::LocateStartLoadPositionInBuildingSegment(
        const OperationCursor& skipCursor, int& segIdx, int& offset)
{
    if (!mBuildingSegment)
    {
        return false;
    }

    if (mBuildingSegment->GetSegmentId() < skipCursor.segId)
    {
        return false;
    }

    segIdx = mSegMetaVec.size() + mDumpingSegments.size();
    
    if (mBuildingSegment->GetSegmentId() == skipCursor.segId)
    {
        offset = skipCursor.pos + 1;
    }
    else
    {
        offset = 0;
    }
    return true;
}

SegmentOperationIteratorPtr OperationIterator::LoadSegIterator(int segmentIdx, int offset)
{
    assert(segmentIdx >= 0);
    assert(offset >= 0);
    
    if (segmentIdx > (int)(mSegDataVec.size() + mDumpingSegments.size()))
    {
        mCurrentSegIdx = segmentIdx;
        return SegmentOperationIteratorPtr();
    }
    
    SegmentOperationIteratorPtr iter;
    if (segmentIdx == (int)(mSegDataVec.size() + mDumpingSegments.size()))
    {
        if (!mBuildingSegment || !(mBuildingSegment->GetOperationWriter()))
        {
            mCurrentSegIdx = segmentIdx;
            return SegmentOperationIteratorPtr();
        }
        OperationWriterPtr operationWriter =
            DYNAMIC_POINTER_CAST(OperationWriter, mBuildingSegment->GetOperationWriter());
        iter = operationWriter->CreateSegmentOperationIterator(
                mBuildingSegment->GetSegmentId(), offset, mTimestamp);
    }
    else if (segmentIdx >= (int)mSegDataVec.size())
    {
        int32_t dumpingIdx = segmentIdx - (int)mSegDataVec.size();
        InMemorySegmentPtr dumpingSegment = mDumpingSegments[dumpingIdx];
        OperationWriterPtr operationWriter =
            DYNAMIC_POINTER_CAST(OperationWriter, dumpingSegment->GetOperationWriter());
        iter = operationWriter->CreateSegmentOperationIterator(
                dumpingSegment->GetSegmentId(), offset, mTimestamp);
    }
    else
    {
        NormalSegmentOperationIteratorPtr normalSegIter(
            new NormalSegmentOperationIterator(
                mSchema,
                mSegMetaVec[segmentIdx],
                mSegDataVec[segmentIdx].GetSegmentId(),
                offset, mTimestamp));

        normalSegIter->Init(mSegDataVec[segmentIdx].GetOperationDirectory(true));
        iter = normalSegIter;
    }
    
    mCurrentSegIdx = segmentIdx;
    UpdateLastCursor(iter);
    if (!iter || !iter->HasNext())
    {
        return SegmentOperationIteratorPtr();
    }
    return iter;
}

void OperationIterator::ExtractUnObsolteRtSegmentDatas(
        const PartitionDataPtr& partitionData, int64_t timestamp,
        SegmentDataVector& segDatas)
{
    SegmentIteratorPtr builtSegIter =
        partitionData->CreateSegmentIterator()->CreateIterator(SIT_BUILT);
    assert(builtSegIter);
    while (builtSegIter->IsValid())
    {
        const SegmentData& segData = builtSegIter->GetSegmentData();
        if (segData.GetSegmentInfo().timestamp < timestamp)
        {
            builtSegIter->MoveToNext();
            continue;            
        }
        DirectoryPtr opDir = segData.GetOperationDirectory(false);
        // only rt segment has operation directory
        if (!opDir)
        {
            builtSegIter->MoveToNext();
            continue;
        }
        segDatas.push_back(segData);
        builtSegIter->MoveToNext();
    }
}

size_t OperationIterator::GetMaxUnObsoleteSegmentOperationSize(
        const PartitionDataPtr& partitionData, int64_t reclaimTimestamp)
{
    SegmentDataVector segDatas;
    ExtractUnObsolteRtSegmentDatas(partitionData, reclaimTimestamp, segDatas);

    OperationMetaVector opMetas;
    LoadOperationMeta(segDatas, opMetas);
    AppendInMemOperationMeta(partitionData, opMetas);
    
    size_t maxOpSegmentSize = 0;
    for (size_t i = 0; i < opMetas.size(); ++i)
    {
        maxOpSegmentSize = max(maxOpSegmentSize, opMetas[i].GetTotalSerializeSize());
    }
    return maxOpSegmentSize;
}

void OperationIterator::LoadOperationMeta(
    const SegmentDataVector& segDataVec, OperationMetaVector& segMetaVec)
{
    for (size_t i = 0; i < segDataVec.size(); ++i)
    {
        const SegmentData& segData = segDataVec[i];
        DirectoryPtr opDir = segData.GetOperationDirectory(true);
        assert(opDir);
        string metaStr;
        opDir->Load(OPERATION_META_FILE_NAME, metaStr);
        OperationMeta meta;
        meta.InitFromString(metaStr);
        assert(meta.GetOperationCount() > 0);
        segMetaVec.push_back(meta);
    }
}

void OperationIterator::AppendInMemOperationMeta(
        const PartitionDataPtr& partitionData, OperationMetaVector& segMetaVec)
{
    SegmentIteratorPtr buildingSegIter =
        partitionData->CreateSegmentIterator()->CreateIterator(SIT_BUILDING);
    assert(buildingSegIter);
    while (buildingSegIter->IsValid())
    {
        OperationWriterPtr opWriter;
        InMemorySegmentPtr inMemSegment = buildingSegIter->GetInMemSegment();
        if (inMemSegment)
        {
            opWriter = DYNAMIC_POINTER_CAST(OperationWriter,
                                            inMemSegment->GetOperationWriter());
        }
        if (opWriter)
        {
            segMetaVec.push_back(opWriter->GetOperationMeta());
        }
        buildingSegIter->MoveToNext();
    }
}

void OperationIterator::InitBuiltSegments(
        const PartitionDataPtr& partitionData, const OperationCursor& skipCursor)
{
    PartitionSegmentIteratorPtr segIter = partitionData->CreateSegmentIterator();
    assert(segIter);
    SegmentIteratorPtr builtSegIter = segIter->CreateIterator(SIT_BUILT);
    while (builtSegIter->IsValid())
    {
        const SegmentData& segData = builtSegIter->GetSegmentData();
        if (segData.GetSegmentInfo().timestamp < mTimestamp
            || segData.GetSegmentId() < skipCursor.segId)
        {
            builtSegIter->MoveToNext();
            continue;            
        }
        DirectoryPtr opDir = segData.GetOperationDirectory(false);
        // only rt segment has operation directory
        if (!opDir)
        {
            builtSegIter->MoveToNext();
            continue;
        }

        IE_LOG(INFO, "load built segment [%d] for redo!", segData.GetSegmentId());
        mSegDataVec.push_back(segData);
        builtSegIter->MoveToNext();
    }
    LoadOperationMeta(mSegDataVec, mSegMetaVec);
}

void OperationIterator::InitInMemSegments(
        const PartitionDataPtr& partitionData, const OperationCursor& skipCursor)
{
    PartitionSegmentIteratorPtr segIter = partitionData->CreateSegmentIterator();
    SegmentIteratorPtr buildingSegIter = segIter->CreateIterator(SIT_BUILDING);
    while (buildingSegIter->IsValid())
    {
        InMemorySegmentPtr inMemSegment = buildingSegIter->GetInMemSegment();
        if (!inMemSegment || !(inMemSegment->GetOperationWriter()))
        {
            buildingSegIter->MoveToNext();
            continue;
        }
        if (inMemSegment->GetStatus() == InMemorySegment::BUILDING)
        {
            if (inMemSegment->GetSegmentId() < skipCursor.segId)
            {
                break;
            }
            mBuildingSegment = inMemSegment;
            IE_LOG(INFO, "load building segment [%d] for redo!",
                   inMemSegment->GetSegmentData().GetSegmentId());
            break;
        }
        // dumping segment
        const SegmentData& segData = buildingSegIter->GetSegmentData();
        if (segData.GetSegmentInfo().timestamp < mTimestamp
            || segData.GetSegmentId() < skipCursor.segId) 
        {
            buildingSegIter->MoveToNext();
            continue;
        }

        IE_LOG(INFO, "load dumping segment [%d] for redo!", segData.GetSegmentId());
        mDumpingSegments.push_back(inMemSegment);
        buildingSegIter->MoveToNext();
    }
}
 
IE_NAMESPACE_END(partition);

