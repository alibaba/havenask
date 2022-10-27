#include "indexlib/index/normal/inverted_index/format/dict_inline_posting_decoder.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DictInlinePostingDecoder);

DictInlinePostingDecoder::DictInlinePostingDecoder(
        const PostingFormatOption& postingFormatOption,
        int64_t dictInlinePostingData)
    : mDictInlineFormatter(postingFormatOption, dictInlinePostingData)
{
    assert(postingFormatOption.IsDictInlineCompress());
}

DictInlinePostingDecoder::~DictInlinePostingDecoder() 
{
}

bool DictInlinePostingDecoder::DecodeDocBuffer(
        docid_t startDocId, docid_t *docBuffer,
        docid_t &firstDocId, docid_t &lastDocId, ttf_t &currentTTF)
{
    assert(mDictInlineFormatter.GetDocFreq() == 1);
    // df == 1, tf == ttf
    lastDocId = mDictInlineFormatter.GetDocId();
    if (startDocId > lastDocId)
    {
        return false;
    }

    docBuffer[0] = mDictInlineFormatter.GetDocId();
    firstDocId = mDictInlineFormatter.GetDocId();
    currentTTF = mDictInlineFormatter.GetTermFreq();
    return true;
}

IE_NAMESPACE_END(index);

