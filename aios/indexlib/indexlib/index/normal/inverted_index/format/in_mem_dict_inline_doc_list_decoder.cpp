#include "indexlib/index/normal/inverted_index/format/in_mem_dict_inline_doc_list_decoder.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemDictInlineDocListDecoder);

void InMemDictInlineDocListDecoder::Init(
        const PostingFormatOption& postingFormatOption,
        int64_t dictInlinePostingData) 
{
    assert(mDictInlineDecoder == NULL);
    mDictInlineDecoder = IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool,
            DictInlinePostingDecoder, postingFormatOption, dictInlinePostingData);
}

bool InMemDictInlineDocListDecoder::DecodeDocBuffer(
        docid_t startDocId, docid_t *docBuffer,
        docid_t &firstDocId, docid_t &lastDocId,
        ttf_t &currentTTF)
{
    if (!mDictInlineDecoder)
    {
        return false;   
    }
    return mDictInlineDecoder->DecodeDocBuffer(startDocId, docBuffer,
            firstDocId, lastDocId, currentTTF);
}

bool InMemDictInlineDocListDecoder::DecodeCurrentTFBuffer(tf_t *tfBuffer)
{
    if (!mDictInlineDecoder)
    {
        return false;   
    }
    return mDictInlineDecoder->DecodeCurrentTFBuffer(tfBuffer);
}

void InMemDictInlineDocListDecoder::DecodeCurrentDocPayloadBuffer(
        docpayload_t *docPayloadBuffer)
{
    if (!mDictInlineDecoder)
    {
        return;   
    }
    return mDictInlineDecoder->DecodeCurrentDocPayloadBuffer(docPayloadBuffer);
}

void InMemDictInlineDocListDecoder::DecodeCurrentFieldMapBuffer(
        fieldmap_t *fieldBitmapBuffer)
{
    if (!mDictInlineDecoder)
    {
        return;   
    }
    return mDictInlineDecoder->DecodeCurrentFieldMapBuffer(fieldBitmapBuffer);
}




IE_NAMESPACE_END(index);

