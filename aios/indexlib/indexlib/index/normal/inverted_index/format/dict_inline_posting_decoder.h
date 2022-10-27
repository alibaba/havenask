#ifndef __INDEXLIB_DICT_INLINE_POSTING_DECODER_H
#define __INDEXLIB_DICT_INLINE_POSTING_DECODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/buffered_segment_index_decoder.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"
#include "indexlib/index/normal/inverted_index/format/dict_inline_formatter.h"

IE_NAMESPACE_BEGIN(index);

class DictInlinePostingDecoder : public index::BufferedSegmentIndexDecoder
{
public:
    DictInlinePostingDecoder(const PostingFormatOption& postingFormatOption,
                             int64_t dictInlinePostingData);

    ~DictInlinePostingDecoder();

public:
    bool DecodeDocBuffer(docid_t startDocId, docid_t *docBuffer,
                         docid_t &firstDocId, docid_t &lastDocId, ttf_t &currentTTF);

    bool DecodeDocBufferMayCopy(docid_t startDocId, docid_t *&docBuffer,
                                docid_t &firstDocId, docid_t &lastDocId, ttf_t &currentTTF)
    { 
        return DecodeDocBuffer(startDocId, docBuffer, firstDocId, lastDocId, currentTTF); 
    }

    bool DecodeCurrentTFBuffer(tf_t *tfBuffer) 
    { 
        assert(tfBuffer);
        tfBuffer[0] = mDictInlineFormatter.GetTermFreq();
        return true; 
    }

    void DecodeCurrentDocPayloadBuffer(docpayload_t *docPayloadBuffer) 
    { 
        assert(docPayloadBuffer);
        docPayloadBuffer[0] = mDictInlineFormatter.GetDocPayload();
    }

    void DecodeCurrentFieldMapBuffer(fieldmap_t *fieldBitmapBuffer) 
    {
        assert(fieldBitmapBuffer);
        fieldBitmapBuffer[0] = mDictInlineFormatter.GetFieldMap();
    }

private:
    DictInlineFormatter mDictInlineFormatter;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DictInlinePostingDecoder);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DICT_INLINE_POSTING_DECODER_H
