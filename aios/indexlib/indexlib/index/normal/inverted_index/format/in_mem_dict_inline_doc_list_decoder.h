#ifndef __INDEXLIB_INMEMDICTINLINEDOCLISTDECODER_H
#define __INDEXLIB_INMEMDICTINLINEDOCLISTDECODER_H

#include <tr1/memory>

#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/in_mem_doc_list_decoder.h"
#include "indexlib/index/normal/inverted_index/format/dict_inline_posting_decoder.h"

IE_NAMESPACE_BEGIN(index);

class InMemDictInlineDocListDecoder : public InMemDocListDecoder
{
public:
    InMemDictInlineDocListDecoder(autil::mem_pool::Pool *sessionPool, bool isReferenceCompress)
        : InMemDocListDecoder(sessionPool, isReferenceCompress)
        , mSessionPool(sessionPool)
        , mDictInlineDecoder(NULL)
    {}
    
    ~InMemDictInlineDocListDecoder()
    {
        if (mDictInlineDecoder)
        {
            IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mDictInlineDecoder);
        }
    }
    
public:
    void Init(const PostingFormatOption& postingFormatOption,
              int64_t dictInlinePostingData);
    
public:
    bool DecodeDocBuffer(docid_t startDocId, docid_t *docBuffer,
            docid_t &firstDocId, docid_t &lastDocId,
            ttf_t &currentTTF) override;

    bool DecodeCurrentTFBuffer(tf_t *tfBuffer) override;

    void DecodeCurrentDocPayloadBuffer(docpayload_t *docPayloadBuffer) override;

    void DecodeCurrentFieldMapBuffer(fieldmap_t *fieldBitmapBuffer) override;

private:
    autil::mem_pool::Pool* mSessionPool;
    DictInlinePostingDecoder* mDictInlineDecoder;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemDictInlineDocListDecoder);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INMEMDICTINLINEDOCLISTDECODER_H
