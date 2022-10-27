#include "indexlib/index/normal/inverted_index/format/dict_inline_formatter.h"
#include "indexlib/misc/exception.h"
#include "indexlib/index/normal/inverted_index/format/dict_inline_encoder.h"
#include "indexlib/index/normal/inverted_index/format/dict_inline_decoder.h"

using namespace std;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DictInlineFormatter);

DictInlineFormatter::DictInlineFormatter(
        const PostingFormatOption& postingFormatOption)
    : mPostingFormatOption(postingFormatOption)
    , mTermPayload(0)
    , mDocId(INVALID_DOCID)
    , mDocPayload(INVALID_DOC_PAYLOAD)
    , mTermFreq(INVALID_TF)
    , mFieldMap(0)
{
}

DictInlineFormatter::DictInlineFormatter(
        const PostingFormatOption& postingFormatOption, uint64_t dictValue)
    : mPostingFormatOption(postingFormatOption)
    , mTermPayload(0)
    , mDocId(INVALID_DOCID)
    , mDocPayload(INVALID_DOC_PAYLOAD)
    , mTermFreq(INVALID_TF)
    , mFieldMap(0)
{
    uint32_t dictItemCount = postingFormatOption.GetDictInlineCompressItemCount();
    assert(dictItemCount > 0);
    
    uint32_t vec[dictItemCount];
    DictInlineDecoder::Decode(dictValue, dictItemCount, vec);
    FromVector(vec, dictItemCount);
}

DictInlineFormatter::DictInlineFormatter(
        const PostingFormatOption& postingFormatOption,
        const vector<uint32_t>& vec)
    : mPostingFormatOption(postingFormatOption)
{
    FromVector((uint32_t*)vec.data(), vec.size());
}

bool DictInlineFormatter::GetDictInlineValue(uint64_t& inlineDictValue)
{
    vector<uint32_t> vec;
    ToVector(vec);
    return DictInlineEncoder::Encode(vec, inlineDictValue);
}

void DictInlineFormatter::ToVector(vector<uint32_t>& vec)
{
    assert(vec.empty());

    if (mPostingFormatOption.HasTermPayload())
    {
        vec.push_back(mTermPayload);
    }

    vec.push_back(mDocId);

    if (mPostingFormatOption.HasDocPayload())
    {
        vec.push_back(mDocPayload);
    }

    if (mPostingFormatOption.HasTfList())
    {
        vec.push_back(mTermFreq);
    }

    if (mPostingFormatOption.HasFieldMap())
    {
        vec.push_back(mFieldMap);
    }
}

void DictInlineFormatter::FromVector(uint32_t* buffer, size_t size)
{
    size_t cursor = 0;
    
    if (mPostingFormatOption.HasTermPayload())
    {
        mTermPayload = (termpayload_t)CheckAndGetValue(cursor, buffer, size);
        cursor++;
    }

    mDocId = (docid_t)CheckAndGetValue(cursor, buffer, size);
    cursor++;

    if (mPostingFormatOption.HasDocPayload())
    {
        mDocPayload = (docpayload_t)CheckAndGetValue(cursor, buffer, size);
        cursor++;
    }

    if (mPostingFormatOption.HasTfList())
    {
        mTermFreq = (tf_t)CheckAndGetValue(cursor, buffer, size);
        cursor++;
    }

    if (mPostingFormatOption.HasFieldMap())
    {
        mFieldMap = (fieldmap_t)CheckAndGetValue(cursor, buffer, size);
        cursor++;
    }

    if (cursor < size)
    {
        INDEXLIB_THROW(misc::RuntimeException, 
                       "DictInLineMapper FromVector failed: "
                       "expect vector elements num is %ld,"
                       "vecotr size is %ld.", cursor, size);
    }
}

uint32_t DictInlineFormatter::CheckAndGetValue(const size_t& cursor,
        uint32_t* buffer, size_t size)
{
    if (cursor >= size)
    {
        INDEXLIB_THROW(misc::OutOfRangeException,
                       "DictInLineMapper get vector value failed: "
                       "expect vector elements idx is %ld,"
                       "vector size is %ld", cursor, size);
    }
    
    return buffer[cursor];
}

uint8_t DictInlineFormatter::CalculateDictInlineItemCount(
        PostingFormatOption& postingFormatOption)
{
    //docid always exist
    uint8_t itemCount = 1;

    if (postingFormatOption.HasTermPayload())
    {
        itemCount++;
    }

    if (postingFormatOption.HasDocPayload())
    {
        itemCount++;
    }

    if (postingFormatOption.HasTfList())
    {
        itemCount++;
    }

    if (postingFormatOption.HasFieldMap())
    {
        itemCount++;
    }
    return itemCount;
}


IE_NAMESPACE_END(index);

