#include "indexlib/index/normal/inverted_index/format/in_mem_doc_list_decoder.h"
#include "indexlib/common/numeric_compress/reference_compress_int_encoder.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemDocListDecoder);

InMemDocListDecoder::InMemDocListDecoder(autil::mem_pool::Pool *sessionPool,
        bool isReferenceCompress) 
{
    mSessionPool = sessionPool;
    mSkipListReader = NULL;
    mDocListBuffer = NULL;
    mDf = 0;
    mFinishDecoded = false;
    mIsReferenceCompress = isReferenceCompress;
}

InMemDocListDecoder::~InMemDocListDecoder() 
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mSkipListReader);
    IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mDocListBuffer);
}

void InMemDocListDecoder::Init(df_t df, index::SkipListReader* skipListReader,
                               BufferedByteSlice* docListBuffer)
{
    mDf = df;
    mSkipListReader = skipListReader;
    mDocListBuffer = docListBuffer;
    mDocListReader.Open(docListBuffer);
}

bool InMemDocListDecoder::DecodeDocBuffer(docid_t startDocId, docid_t *docBuffer,
        docid_t &firstDocId, docid_t &lastDocId, ttf_t &currentTTF)
{
    DocBufferInfo docBufferInfo(docBuffer, firstDocId, lastDocId, currentTTF);
    if (mSkipListReader == NULL)
    {
        currentTTF = 0;
        return DecodeDocBufferWithoutSkipList(0, 0, startDocId, docBufferInfo);
    }

    uint32_t offset;
    uint32_t recordLen;
    uint32_t lastDocIdInPrevRecord;

    if (!mSkipListReader->SkipTo((uint32_t)startDocId, (uint32_t&)lastDocId,
                                 lastDocIdInPrevRecord, offset, recordLen))
    {
        // we should decode buffer
        lastDocIdInPrevRecord = mSkipListReader->GetLastKeyInBuffer();
        offset = mSkipListReader->GetLastValueInBuffer();
        currentTTF = mSkipListReader->GetCurrentTTF();
        mSkipedItemCount = mSkipListReader->GetSkippedItemCount();
        return DecodeDocBufferWithoutSkipList(lastDocIdInPrevRecord, offset,
                startDocId, docBufferInfo);
    }

    mSkipedItemCount = mSkipListReader->GetSkippedItemCount();
    currentTTF = mSkipListReader->GetPrevTTF();
    mDocListReader.Seek(offset);

    size_t acutalDecodeCount = 0;
    mDocListReader.Decode(docBuffer, MAX_DOC_PER_RECORD, acutalDecodeCount);
    
    if (mIsReferenceCompress)
    {
        firstDocId = ReferenceCompressInt32Encoder::GetFirstValue((char*)docBuffer);
    }
    else
    {
        firstDocId = docBuffer[0] + lastDocIdInPrevRecord;
    }
    return true;
}

bool InMemDocListDecoder::DecodeDocBufferWithoutSkipList(docid_t lastDocIdInPrevRecord,
        uint32_t offset, docid_t startDocId, DocBufferInfo& docBufferInfo)
{
    // only decode one time
    if (mFinishDecoded)
    {
        return false;
    }

    //TODO: seek return value
    mDocListReader.Seek(offset);

    // short list when no skip 
    size_t decodeCount;
    if (!mDocListReader.Decode(docBufferInfo.docBuffer, MAX_DOC_PER_RECORD, decodeCount))
    {
        return false;
    }

    if (mIsReferenceCompress)
    {
        assert(decodeCount > 0);
        ReferenceCompressIntReader<uint32_t> bufferReader;
        bufferReader.Reset((char*)docBufferInfo.docBuffer);

        docBufferInfo.lastDocId = bufferReader[decodeCount - 1];
        docBufferInfo.firstDocId = bufferReader[0];
    }
    else
    {
        docBufferInfo.lastDocId = lastDocIdInPrevRecord;
        for (size_t i = 0; i < decodeCount; ++i)
        {
            docBufferInfo.lastDocId += docBufferInfo.docBuffer[i];
        }
        docBufferInfo.firstDocId = docBufferInfo.docBuffer[0] + lastDocIdInPrevRecord;
    }
    if (startDocId > docBufferInfo.lastDocId)
    {
        return false;
    }

    mFinishDecoded = true;
    return true;
}

bool InMemDocListDecoder::DecodeCurrentTFBuffer(tf_t *tfBuffer)
{
    size_t decodeCount;
    return mDocListReader.Decode(tfBuffer, MAX_DOC_PER_RECORD, decodeCount);
}

void InMemDocListDecoder::DecodeCurrentDocPayloadBuffer(
        docpayload_t *docPayloadBuffer)
{
    size_t decodeCount;
    mDocListReader.Decode(docPayloadBuffer, MAX_DOC_PER_RECORD, decodeCount);
}

void InMemDocListDecoder::DecodeCurrentFieldMapBuffer(
        fieldmap_t *fieldBitmapBuffer)
{
    size_t decodeCount;
    mDocListReader.Decode(fieldBitmapBuffer, MAX_DOC_PER_RECORD, decodeCount);
}

IE_NAMESPACE_END(index);

