
#include "indexlib/index/normal/inverted_index/accessor/posting_merger_impl.h"
#include "indexlib/index/normal/inverted_index/format/one_doc_merger.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"
#include "indexlib/common/numeric_compress/vbyte_compressor.h"
#include "indexlib/index/normal/inverted_index/format/term_meta_dumper.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"

using namespace std;
using namespace std::tr1;
using namespace autil::mem_pool;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PostingMergerImpl);

class PostingListSortedItem
{
public:
    PostingListSortedItem(
            const PostingFormatOption& formatOption, 
            docid_t baseDocId,
            const ReclaimMapPtr &reclaimMap,
            PostingDecoderImpl *decoder)
        : mBaseDocId(baseDocId)
        , mNewDocId(INVALID_DOCID)
        , mReclaimMap(reclaimMap)
        , mOneDocMerger(formatOption, decoder)
    {}

public:
    bool Next()
    {
        docid_t nextDocId = mOneDocMerger.NextDoc();
        if (nextDocId == INVALID_DOCID)
        {
            return false;
        }
        mNewDocId = mReclaimMap->GetNewId(mBaseDocId + nextDocId);
        return true;
    }

    void MergeDoc(const MultiSegmentPostingWriterPtr &posWriter)
    {
        if (mNewDocId == INVALID_DOCID)
        {
            mOneDocMerger.Merge(INVALID_DOCID, NULL);
            return;
        }
        pair<segmentindex_t, docid_t> docidInfo = mReclaimMap->GetLocalId(mNewDocId);
        PostingWriterImplPtr postingWriterPtr =
            DYNAMIC_POINTER_CAST(PostingWriterImpl,
                                 posWriter->GetSegmentPostingWriter(docidInfo.first));
        mOneDocMerger.Merge(docidInfo.second, postingWriterPtr.get());
    }

    docid_t GetDocId() const
    {
        return mNewDocId;
    }

private:
    docid_t mBaseDocId;
    docid_t mNewDocId;
    ReclaimMapPtr mReclaimMap;
    OneDocMerger mOneDocMerger;
};

struct PostingListSortedItemComparator
{
    bool operator()(const PostingListSortedItem* item1,
                    const PostingListSortedItem* item2)
    {
        return (item1->GetDocId() > item2->GetDocId());
    }
};

typedef priority_queue<PostingListSortedItem*,
                       vector<PostingListSortedItem*>,
                       PostingListSortedItemComparator> SortWeightQueue;

/////////////////////////////////////////////////////////////////////////////
PostingMergerImpl::PostingMergerImpl(PostingWriterResource* postingWriterResource,
    const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos, bool disableDictInline)
    : mDf(-1)
    , mttf(-1)
{
    assert(postingWriterResource);
    mPostingFormatOption = postingWriterResource->postingFormatOption;
    mPostingWriter = 
        CreatePostingWriter(postingWriterResource,
                            outputSegmentMergeInfos, disableDictInline);
}

MultiSegmentPostingWriterPtr PostingMergerImpl::CreatePostingWriter(
        PostingWriterResource* postingWriterResource,
        const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos,
        bool disableDictInline)
{
    MultiSegmentPostingWriterPtr multiSegmentPostingWriterPtr(
            new MultiSegmentPostingWriter(
                    postingWriterResource, outputSegmentMergeInfos,
                    mPostingFormatOption, disableDictInline));
    return multiSegmentPostingWriterPtr;
}

PostingMergerImpl::~PostingMergerImpl() 
{
}

void PostingMergerImpl::Merge(
        const SegmentTermInfos& segTermInfos,
        const ReclaimMapPtr& reclaimMap)
{
    for (size_t i = 0; i < segTermInfos.size(); i++)
    {
        SegmentTermInfo *segInfo = segTermInfos[i];
        docid_t baseDocId = segInfo->baseDocId;
        PostingDecoderImpl *decoder =
            dynamic_cast<PostingDecoderImpl*>(segInfo->postingDecoder);
        assert(decoder != NULL);
        mTermPayload = decoder->GetTermMeta()->GetPayload();
        MergeOneSegment(reclaimMap, decoder, baseDocId);
    }
    mPostingWriter->EndSegment();
}

void PostingMergerImpl::MergeOneSegment(
        const ReclaimMapPtr& reclaimMap,
        PostingDecoderImpl *decoder, docid_t baseDocId)
{
    OneDocMerger oneDocMerger(mPostingFormatOption, decoder);
    while (true)
    {
        docid_t oldDocId = oneDocMerger.NextDoc();
        if (oldDocId == INVALID_DOCID)
        {
            break;
        }
        pair<segmentindex_t, docid_t> docidInfo = reclaimMap->GetNewLocalId(baseDocId + oldDocId);
        if (docidInfo.second == INVALID_DOCID)
        {
            oneDocMerger.Merge(INVALID_DOCID, NULL);
        }
        else
        {
            PostingWriterImplPtr postingWriter =
                DYNAMIC_POINTER_CAST(PostingWriterImpl,
                                     mPostingWriter->GetSegmentPostingWriter(docidInfo.first));
            oneDocMerger.Merge(docidInfo.second, postingWriter.get());
        }
    }
}

void PostingMergerImpl::SortByWeightMerge(
        const SegmentTermInfos& segTermInfos,
        const ReclaimMapPtr& reclaimMap)
{
    SortWeightQueue queue;
    for (size_t i = 0; i < segTermInfos.size(); ++i)
    {
        SegmentTermInfo *segInfo = segTermInfos[i];
        docid_t baseDocId = segInfo->baseDocId;
        PostingDecoderImpl *decoder =
            dynamic_cast<PostingDecoderImpl*>(segInfo->postingDecoder);
        mTermPayload = decoder->GetTermMeta()->GetPayload();
        
        PostingListSortedItem* queueItem = new PostingListSortedItem(
                mPostingFormatOption, baseDocId, reclaimMap, decoder);
        if (queueItem->Next())
        {
            queue.push(queueItem);
        }
        else
        {
            delete queueItem;
        }
    }

    while (!queue.empty())
    {
        PostingListSortedItem* item = queue.top();
        queue.pop();
        item->MergeDoc(mPostingWriter);
        if (item->Next())
        {
            queue.push(item);
        }
        else
        {
            delete item;
        }
    }
    mPostingWriter->EndSegment();
}

void PostingMergerImpl::Dump(dictkey_t key, IndexOutputSegmentResources& indexOutputSegmentResources)
{
    mPostingWriter->Dump(key, indexOutputSegmentResources, mTermPayload);
}

uint64_t PostingMergerImpl::GetPostingLength() const 
{ 
    return mPostingWriter->GetPostingLength();
}

IE_NAMESPACE_END(index);

