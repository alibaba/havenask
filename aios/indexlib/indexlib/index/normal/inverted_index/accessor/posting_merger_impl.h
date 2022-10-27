#ifndef __INDEXLIB_POSTING_MERGER_IMPL_H
#define __INDEXLIB_POSTING_MERGER_IMPL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer_impl.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_segment_posting_writer.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"
#include "indexlib/index/normal/inverted_index/format/posting_format.h"
#include "indexlib/index/normal/inverted_index/format/posting_decoder_impl.h"
#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/RecyclePool.h>

DECLARE_REFERENCE_STRUCT(index, PostingWriterResource);

IE_NAMESPACE_BEGIN(index);

class PostingMergerImpl : public index::PostingMerger
{
public:
    PostingMergerImpl(
            index::PostingWriterResource* postingWriterResource,
            const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos,
            bool disableDictInline = false);
                      
    ~PostingMergerImpl();

public:
    void Merge(
        const index::SegmentTermInfos& segTermInfos, const index::ReclaimMapPtr& reclaimMap);

    void SortByWeightMerge(
        const index::SegmentTermInfos& segTermInfos, const index::ReclaimMapPtr& reclaimMap);

    void Dump(dictkey_t key, IndexOutputSegmentResources& indexOutputSegmentResources);

    uint64_t GetPostingLength() const ;
    
    ttf_t GetTotalTF()  override
    {
        if (mttf == -1)
        {
            mttf = mPostingWriter->GetTotalTF();
        }
        return mttf;
    }

    df_t GetDocFreq()  override
    {
        if (mDf == -1)
        {
            mDf = mPostingWriter->GetDF();
        }
        return mDf;
    }

    bool IsCompressedPostingHeader() const  { return true; }

private:
    void MergeOneSegment(const index::ReclaimMapPtr& reclaimMap,
                         PostingDecoderImpl *decoder, docid_t baseDocId);

    MultiSegmentPostingWriterPtr CreatePostingWriter(
            PostingWriterResource* postingWriterResource,
            const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos,
            bool disableDictInline);

    PostingIteratorPtr CreatePostingIterator() override
    {
        return mPostingWriter->CreatePostingIterator(mTermPayload);
    }

private:
    PostingFormatOption mPostingFormatOption;
    MultiSegmentPostingWriterPtr mPostingWriter;
    df_t mDf;
    ttf_t mttf;

private:
    friend class PostingMergerImplTest;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PostingMergerImpl);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSTING_MERGER_IMPL_H
