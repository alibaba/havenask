#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_merger.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_writer.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_index_sorted_queue_item.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BitmapPostingMerger);



BitmapPostingMerger::BitmapPostingMerger(Pool* pool,
        const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos,
        optionflag_t optionFlag)
    : mPool(pool)
    , mWriter(pool, outputSegmentMergeInfos)
    , mDf(-1)
    , mOptionFlag(optionFlag)
{
    (void)mPool;
}

BitmapPostingMerger::~BitmapPostingMerger() 
{
}

void BitmapPostingMerger::Merge(const SegmentTermInfos& segTermInfos,
                                const ReclaimMapPtr& reclaimMap)
{
    for (size_t i = 0; i < segTermInfos.size(); ++i)
    {
        const SegmentTermInfo* segTermInfo = segTermInfos[i];
        BitmapPostingDecoder* postDecoder = 
            dynamic_cast<BitmapPostingDecoder*>(segTermInfo->postingDecoder);

        const TermMeta *termMeta = postDecoder->GetTermMeta();
        mTermPayload = termMeta->GetPayload();
        
        docid_t baseDocId = segTermInfo->baseDocId;
        docid_t docBuffer[DEFAULT_BUFFER_SIZE];
        int32_t decodedNum;

        while(1)
        {
            decodedNum = postDecoder->DecodeDocList(docBuffer, DEFAULT_BUFFER_SIZE);
            if (decodedNum <= 0)
            {
                break;
            }
            for (uint32_t i = 0; i < (uint32_t)decodedNum; ++i)
            {
                docid_t oldId = baseDocId + docBuffer[i];
                docid_t newId = reclaimMap->GetNewId(oldId);
                if (newId != INVALID_DOCID)
                {
                    pair<segmentindex_t, docid_t> docidInfo = reclaimMap->GetLocalId(newId);
                    auto postingWriter = mWriter.GetSegmentBitmapPostingWriter(docidInfo.first);
                    postingWriter->EndDocument(docidInfo.second, 0);
                }
            }
        }
    }
}

void BitmapPostingMerger::SortByWeightMerge(const SegmentTermInfos& segTermInfos,
        const ReclaimMapPtr& reclaimMap)
{
    SortByWeightQueue sortByWeightQueue;
    for (size_t i = 0; i < segTermInfos.size(); ++i)
    {
        BitmapIndexSortedQueueItem* queueItem = new BitmapIndexSortedQueueItem(
                *(segTermInfos[i]), reclaimMap);
        if (queueItem->Next())
        {
            sortByWeightQueue.push(queueItem);
            mTermPayload = queueItem->GetTermMeta()->GetPayload();
        }
        else
        {
            delete queueItem;
        }
    }

    while (!sortByWeightQueue.empty())
    {
        BitmapIndexSortedQueueItem* item = sortByWeightQueue.top();
        sortByWeightQueue.pop();
        docid_t newId = item->GetDocId();
        pair<segmentindex_t, docid_t> docidInfo = reclaimMap->GetLocalId(newId);
        auto postingWriter = mWriter.GetSegmentBitmapPostingWriter(docidInfo.first);
        postingWriter->EndDocument(docidInfo.second, 0);

        if (item->Next())
        {
            sortByWeightQueue.push(item);
        }
        else
        {
            delete item;
        }
    }
}

void BitmapPostingMerger::Dump(dictkey_t key, IndexOutputSegmentResources& indexOutputSegmentResources)
{
    mWriter.Dump(key, indexOutputSegmentResources, mTermPayload);
}

IE_NAMESPACE_END(index);

