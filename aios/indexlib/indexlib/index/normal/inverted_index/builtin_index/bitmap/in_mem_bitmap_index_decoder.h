#ifndef __INDEXLIB_IN_MEM_BITMAP_INDEX_DECODER_H
#define __INDEXLIB_IN_MEM_BITMAP_INDEX_DECODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"

DECLARE_REFERENCE_CLASS(index, BitmapPostingWriter);

IE_NAMESPACE_BEGIN(index);

class InMemBitmapIndexDecoder
{
public:
    InMemBitmapIndexDecoder();
    ~InMemBitmapIndexDecoder();

public:
    void Init(const BitmapPostingWriter* postingWriter);

    const index::TermMeta* GetTermMeta() const { return &mTermMeta; }

    uint32_t GetBitmapItemCount() const { return mBitmapItemCount; }
    uint32_t* GetBitmapData() const { return mBitmapData; }

private:
    index::TermMeta mTermMeta;
    uint32_t mBitmapItemCount;
    uint32_t* mBitmapData;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemBitmapIndexDecoder);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_IN_MEM_BITMAP_INDEX_DECODER_H
