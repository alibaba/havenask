#ifndef __INDEXLIB_BITMAP_POSTING_MERGER_H
#define __INDEXLIB_BITMAP_POSTING_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger.h"
#include <autil/mem_pool/Pool.h>
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_writer.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/multi_segment_bitmap_posting_writer.h"

IE_NAMESPACE_BEGIN(index);

class BitmapPostingMerger : public PostingMerger
{
public:
    BitmapPostingMerger(autil::mem_pool::Pool* pool,
                        const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos,
                        optionflag_t optionFlag);
    ~BitmapPostingMerger();

public:
    void Merge(const SegmentTermInfos& segTermInfos,
               const ReclaimMapPtr& reclaimMap) override;

    void SortByWeightMerge(const SegmentTermInfos& segTermInfos,
                           const ReclaimMapPtr& reclaimMap) override;

    void Dump(dictkey_t key, IndexOutputSegmentResources& indexOutputSegmentResources) override;
    
    df_t GetDocFreq()  override 
    {
        if (mDf == -1)
        {
            mDf = mWriter.GetDF();
        }
        return mDf;
    }

    ttf_t GetTotalTF()  override { return 0; }

    uint64_t GetPostingLength() const override
    {
        return mWriter.GetDumpLength();
    }

    bool IsCompressedPostingHeader() const override { return false; }

    PostingIteratorPtr CreatePostingIterator() override
    {
        return mWriter.CreatePostingIterator(mOptionFlag, mTermPayload);
    }

private:
    static const uint32_t DEFAULT_BUFFER_SIZE = 1024 * 8;
    autil::mem_pool::Pool* mPool;
    MultiSegmentBitmapPostingWriter mWriter;
    df_t mDf;
    optionflag_t mOptionFlag;

    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BitmapPostingMerger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BITMAP_POSTING_MERGER_H
