#include "indexlib/index/normal/inverted_index/accessor/segment_term_info_queue.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/on_disk_bitmap_index_iterator.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index_base/partition_data.h"

using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SegmentTermInfoQueue);

SegmentTermInfoQueue::SegmentTermInfoQueue(
        const IndexConfigPtr& indexConf,
        const OnDiskIndexIteratorCreatorPtr& onDiskIndexIterCreator)
    : mIndexConfig(indexConf)
    , mOnDiskIndexIterCreator(onDiskIndexIterCreator)
{
}

SegmentTermInfoQueue::~SegmentTermInfoQueue() 
{
    while (!mSegmentTermInfos.empty())
    {
        SegmentTermInfo* segTermInfo = mSegmentTermInfos.top();
        mSegmentTermInfos.pop();
        delete segTermInfo;
    }
    for (size_t i = 0; i < mMergingSegmentTermInfos.size(); i++)
    {
        DELETE_AND_SET_NULL(mMergingSegmentTermInfos[i]);
    }
}

void SegmentTermInfoQueue::Init(
        const SegmentDirectoryBasePtr& segDir,
        const SegmentMergeInfos& segMergeInfos)
{
    mSegmentDirectory = segDir;
    for (size_t i = 0; i < segMergeInfos.size(); ++i)
    {
        AddOnDiskTermInfo(segMergeInfos[i]);
    }
}

void SegmentTermInfoQueue::AddOnDiskTermInfo(
        const SegmentMergeInfo& segMergeInfo)
{
    IndexIteratorPtr indexIt;
    indexIt = CreateOnDiskNormalIterator(segMergeInfo);
    AddQueueItem(segMergeInfo, indexIt, SegmentTermInfo::TM_NORMAL);

    if (mIndexConfig->GetHighFreqVocabulary())
    {
        indexIt = CreateOnDiskBitmapIterator(segMergeInfo);
        AddQueueItem(segMergeInfo, indexIt, SegmentTermInfo::TM_BITMAP);        
    }
}

void SegmentTermInfoQueue::AddQueueItem(
        const SegmentMergeInfo& segMergeInfo,
        const IndexIteratorPtr& indexIt,
        SegmentTermInfo::TermIndexMode mode)
{
    if (indexIt)
    {
        unique_ptr<SegmentTermInfo> queItem(new SegmentTermInfo(
                        segMergeInfo.segmentId, segMergeInfo.baseDocId,
                        indexIt, mode));
        if (queItem->Next())
        {
            mSegmentTermInfos.push(queItem.release());
        }
    }
}

const SegmentTermInfos& SegmentTermInfoQueue::CurrentTermInfos(
        dictkey_t& key, SegmentTermInfo::TermIndexMode &termMode)
{
    assert(mMergingSegmentTermInfos.empty());
    SegmentTermInfo* item = mSegmentTermInfos.top();
    mSegmentTermInfos.pop();

    key = item->key;
    termMode = item->termIndexMode;

    mMergingSegmentTermInfos.push_back(item);

    item = (mSegmentTermInfos.size() > 0) ? mSegmentTermInfos.top() : NULL;
    while (item != NULL && key == item->key)
    {
        if (item->termIndexMode != termMode)
        {
            break;
        }
        mMergingSegmentTermInfos.push_back(item);
        mSegmentTermInfos.pop();
        item = (mSegmentTermInfos.size() > 0) ? mSegmentTermInfos.top() : NULL;
    }
    return mMergingSegmentTermInfos;
}

void SegmentTermInfoQueue::MoveToNextTerm()
{
    for (size_t i = 0; i < mMergingSegmentTermInfos.size(); ++i)
    {
        SegmentTermInfo* itemPtr = mMergingSegmentTermInfos[i];
        assert(itemPtr);
        if (itemPtr->Next())
        {
            mMergingSegmentTermInfos[i] = NULL;
            mSegmentTermInfos.push(itemPtr);
        }
        else
        {
            DELETE_AND_SET_NULL(mMergingSegmentTermInfos[i]);
        }
    }
    mMergingSegmentTermInfos.clear();
}

IndexIteratorPtr SegmentTermInfoQueue::CreateOnDiskNormalIterator(
        const SegmentMergeInfo& segMergeInfo) const
{
    PartitionDataPtr partitionData = mSegmentDirectory->GetPartitionData();
    assert(partitionData);

    SegmentData segData = partitionData->GetSegmentData(segMergeInfo.segmentId);
    
    OnDiskIndexIterator* onDiskIndexIter = mOnDiskIndexIterCreator->Create(segData);
    IndexIteratorPtr indexIt;
    if (onDiskIndexIter)
    {
        indexIt.reset(onDiskIndexIter);
        onDiskIndexIter->Init();        
    }
    return indexIt;
}

IndexIteratorPtr SegmentTermInfoQueue::CreateOnDiskBitmapIterator(
            const SegmentMergeInfo& segMergeInfo) const
{
    PartitionDataPtr partitionData = mSegmentDirectory->GetPartitionData();
    assert(partitionData);

    SegmentData segData = partitionData->GetSegmentData(segMergeInfo.segmentId);
    DirectoryPtr indexDirectory = segData.GetIndexDirectory(
            mIndexConfig->GetIndexName(), false);

    IndexIteratorPtr indexIt;
    if (indexDirectory && indexDirectory->IsExist(BITMAP_DICTIONARY_FILE_NAME))
    {
        OnDiskIndexIterator* onDiskIndexIter = 
            mOnDiskIndexIterCreator->CreateBitmapIterator(indexDirectory);
        
        indexIt.reset(onDiskIndexIter);
        onDiskIndexIter->Init();
    }
    return indexIt;
}

IE_NAMESPACE_END(index);

