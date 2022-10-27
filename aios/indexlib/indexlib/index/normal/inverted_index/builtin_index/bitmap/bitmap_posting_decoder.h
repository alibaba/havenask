#ifndef __INDEXLIB_BITMAP_POSTING_DECODER_H
#define __INDEXLIB_BITMAP_POSTING_DECODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/posting_decoder.h"
#include "indexlib/common/byte_slice_reader.h"
#include "indexlib/util/bitmap.h"

IE_NAMESPACE_BEGIN(index);

class BitmapPostingDecoder : public PostingDecoder
{
public:
    BitmapPostingDecoder();
    ~BitmapPostingDecoder();

public:
    void Init(index::TermMeta *termMeta,
              uint8_t* data, uint32_t size);
    uint32_t DecodeDocList(docid_t* docBuffer, size_t len);

private:
    util::BitmapPtr mBitmap;
    docid_t mDocIdCursor;
    docid_t mEndDocId;

    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BitmapPostingDecoder);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BITMAP_POSTING_DECODER_H
