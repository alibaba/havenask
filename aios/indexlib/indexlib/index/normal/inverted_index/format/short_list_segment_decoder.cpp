#include "indexlib/index/normal/inverted_index/format/short_list_segment_decoder.h"

using namespace std;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, ShortListSegmentDecoder);

ShortListSegmentDecoder::ShortListSegmentDecoder(
        common::ByteSliceReader *docListReader,
        uint32_t docListBegin,
        uint8_t docCompressMode)
    : mDecodeShortList(false)
    , mDocListReader(docListReader)
    , mDocListBeginPos(docListBegin)
{
    mDocEncoder = EncoderProvider::GetInstance()->GetDocListEncoder(
            docCompressMode);
    mTfEncoder = EncoderProvider::GetInstance()->GetTfListEncoder(
            docCompressMode);
    mDocPayloadEncoder = EncoderProvider::GetInstance()->GetDocPayloadEncoder(
            docCompressMode);
    mFieldMapEncoder = EncoderProvider::GetInstance()->GetFieldMapEncoder(
            docCompressMode);    
}

ShortListSegmentDecoder::~ShortListSegmentDecoder() 
{
}

bool ShortListSegmentDecoder::DecodeDocBuffer(docid_t startDocId, docid_t *docBuffer,
        docid_t &firstDocId, docid_t &lastDocId, ttf_t &currentTTF)
{
    if (mDecodeShortList)
    {
        // only decode once. for performance purpose
        return false;
    }
    mDecodeShortList = true;
    currentTTF = 0;
    mDocListReader->Seek(mDocListBeginPos);
    uint32_t docNum = mDocEncoder->Decode((uint32_t*)docBuffer,
            MAX_DOC_PER_RECORD, *mDocListReader);
    lastDocId = 0;
    for (uint32_t i = 0; i < docNum; ++i)
    {
        lastDocId += docBuffer[i];
    }

    if (startDocId > lastDocId)
    {
        return false;
    }

    firstDocId = docBuffer[0];
    return true;
}

bool ShortListSegmentDecoder::DecodeDocBufferMayCopy(docid_t startDocId, 
        docid_t *&docBuffer, docid_t &firstDocId, docid_t &lastDocId, ttf_t &currentTTF)
{
    if (mDecodeShortList)
    {
        // only decode once. for performance purpose
        return false;
    }
    mDecodeShortList = true;
    currentTTF = 0;
    mDocListReader->Seek(mDocListBeginPos);
    uint32_t docNum = mDocEncoder->DecodeMayCopy((uint32_t*&)docBuffer,
            MAX_DOC_PER_RECORD, *mDocListReader);
    lastDocId = docNum > 0 ? docBuffer[docNum - 1] : 0;

    if (startDocId > lastDocId)
    {
        return false;
    }

    firstDocId = docBuffer[0];
    return true;
}

IE_NAMESPACE_END(index);

