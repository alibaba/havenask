#include "indexlib/index/normal/inverted_index/accessor/index_term_extender.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_posting_iterator_creator.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger.h"
#include "indexlib/index/normal/adaptive_bitmap/multi_adaptive_bitmap_index_writer.h"
#include "indexlib/common/in_mem_file_writer.h"
#include "indexlib/config/index_config.h"

using namespace std;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, IndexTermExtender);

IndexTermExtender::IndexTermExtender(
        const config::IndexConfigPtr& indexConfig,
        const TruncateIndexWriterPtr& truncateIndexWriter,
        const MultiAdaptiveBitmapIndexWriterPtr& adaptiveBitmapWriter)
    : mIndexConfig(indexConfig)
    , mTruncateIndexWriter(truncateIndexWriter)
    , mAdaptiveBitmapIndexWriter(adaptiveBitmapWriter)
{
    mIndexFormatOption.Init(indexConfig);    
}

IndexTermExtender::~IndexTermExtender() 
{
}

void IndexTermExtender::Init(
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
        IndexOutputSegmentResources& indexOutputSegmentResources)
{
    if (mTruncateIndexWriter)
    {
        mTruncateIndexWriter->Init(outputSegMergeInfos);
    }
    if (mAdaptiveBitmapIndexWriter)
    {
        mAdaptiveBitmapIndexWriter->Init(indexOutputSegmentResources);
    }
}

IndexTermExtender::TermOperation IndexTermExtender::ExtendTerm(
        dictkey_t key, 
        const PostingMergerPtr& postingMerger,
        SegmentTermInfo::TermIndexMode mode)
{
    bool truncateRemain = true;
    bool adaptiveRemain = true;

    if (mTruncateIndexWriter)
    {
        truncateRemain = MakeTruncateTermPosting(key, postingMerger, mode);
    }

    if (mAdaptiveBitmapIndexWriter && mode != SegmentTermInfo::TM_BITMAP)
    {
        adaptiveRemain = MakeAdaptiveBitmapTermPosting(key, postingMerger);
    }
    
    if (truncateRemain && adaptiveRemain)
    {
        return TO_REMAIN;
    }
    return TO_DISCARD;
}

void IndexTermExtender::Destroy()
{
    if (mAdaptiveBitmapIndexWriter)
    {
        mAdaptiveBitmapIndexWriter->EndPosting();
    }

    if (mTruncateIndexWriter)
    {
        mTruncateIndexWriter->EndPosting();
    }
}

bool IndexTermExtender::MakeTruncateTermPosting(
        dictkey_t key, 
        const PostingMergerPtr& postingMerger,
        SegmentTermInfo::TermIndexMode mode)
{
    bool remainPosting = true;
    df_t df = postingMerger->GetDocFreq();
    TruncateTriggerInfo triggerInfo(key, df);
    if (mTruncateIndexWriter->NeedTruncate(triggerInfo))
    {
        if (mode == SegmentTermInfo::TM_NORMAL 
            && mIndexConfig->IsBitmapOnlyTerm(key))
        {
            // fixbug #258648
            int32_t truncPostingCount = 
                mTruncateIndexWriter->GetEstimatePostingCount();
            int32_t truncMetaPostingCount = 0;
            mIndexConfig->GetTruncatePostingCount(key, truncMetaPostingCount);
            if (truncPostingCount >= truncMetaPostingCount)
            {
                // only bitmap && normal term only exist in no truncate posting segment
                remainPosting = false;
            }
            // Do not do truncate, bitmap term will cover
            return remainPosting;
        }
        
        PostingIteratorPtr postingIterator = postingMerger->CreatePostingIterator();
        PostingIteratorPtr postingIteratorClone(postingIterator->Clone());
        TruncatePostingIteratorPtr truncatePostingIterator(
                new TruncatePostingIterator(postingIterator, postingIteratorClone));
        const string &indexName = mIndexConfig->GetIndexName();
        IE_LOG(DEBUG, "Begin generating truncate list for %s:%lu", 
               indexName.c_str(), key);
        mTruncateIndexWriter->AddPosting(key, truncatePostingIterator, postingMerger->GetDocFreq());
        IE_LOG(DEBUG, "End generating truncate list for %s:%lu", 
               indexName.c_str(), key);
    }
    return remainPosting;
}

bool IndexTermExtender::MakeAdaptiveBitmapTermPosting(
        dictkey_t key, 
        const PostingMergerPtr& postingMerger)
{
    bool isAdaptive = mAdaptiveBitmapIndexWriter->NeedAdaptiveBitmap(
            key, postingMerger);

    bool remainPosting = true;
    if (isAdaptive)
    {
        PostingIteratorPtr postingIter = postingMerger->CreatePostingIterator();
        assert(postingIter);

        const string &indexName = mIndexConfig->GetIndexName();
        IE_LOG(DEBUG, "Begin generating adaptive bitmap list for %s:%lu", 
               indexName.c_str(), key);
        mAdaptiveBitmapIndexWriter->AddPosting(
                key, postingMerger->GetTermPayload(), postingIter);
        IE_LOG(DEBUG, "End generating adaptive bitmap list for %s:%lu", 
               indexName.c_str(), key);

        if (mIndexConfig->GetHighFrequencyTermPostingType() == hp_bitmap)
        {
            remainPosting = false;
        }
    }
    return remainPosting;
}

PostingIteratorPtr IndexTermExtender::CreateCommonPostingIterator(
        const util::ByteSliceListPtr& sliceList, 
        uint8_t compressMode,
        SegmentTermInfo::TermIndexMode mode)
{
    PostingFormatOption postingFormatOption = mIndexFormatOption.GetPostingFormatOption();
    if (mode == SegmentTermInfo::TM_BITMAP)
    {
        postingFormatOption = postingFormatOption.GetBitmapPostingFormatOption();
    }
    
    SegmentPosting segPosting(postingFormatOption);
    segPosting.Init(compressMode, sliceList, 0, SegmentInfo());

    return TruncatePostingIteratorCreator::Create(
            mIndexConfig, segPosting, mode == SegmentTermInfo::TM_BITMAP);
}

IE_NAMESPACE_END(index);

