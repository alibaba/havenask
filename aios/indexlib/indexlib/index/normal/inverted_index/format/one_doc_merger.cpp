#include "indexlib/index/normal/inverted_index/format/one_doc_merger.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, OneDocMerger);

OneDocMerger::OneDocMerger(
        const PostingFormatOption& formatOption, 
        PostingDecoderImpl *decoder)
    : mDocCountInBuf(0)
    , mDocBufCursor(0)
    , mDocId(0)
    , mPosCountInBuf(0)
    , mPosBufCursor(0)
    , mPosIndex(0)
    , mDecoder(decoder)
    , mFormatOption(formatOption)
{
    assert(mDecoder);
}

OneDocMerger::~OneDocMerger() 
{
}

docid_t OneDocMerger::NextDoc()
{
    if (mDocBufCursor == mDocCountInBuf)
    {
        mDocBufCursor = 0;
        mDocCountInBuf = mDecoder->DecodeDocList(
                mDocIdBuf, mTfListBuf, mDocPayloadBuf, 
                mFieldMapBuffer, MAX_DOC_PER_RECORD);

        if (mDocCountInBuf == 0)
        {
            return INVALID_DOCID;
        }
        mBufReader.Reset((char*)mDocIdBuf);
    }
    if (mFormatOption.IsReferenceCompress())
    {
        return mBufReader[mDocBufCursor];
    }
    else
    {
        return mDocId + mDocIdBuf[mDocBufCursor];
    }
}

void OneDocMerger::Merge(docid_t newDocId, 
                         PostingWriterImpl* postingWriter)
{
    // No need to decode docid buffer since the buffer has been checked
    // by NextDoc()
    assert(mDocBufCursor < mDocCountInBuf);
    if (!mFormatOption.IsReferenceCompress())
    {
        mDocId += mDocIdBuf[mDocBufCursor];
    }
    tf_t tf = MergePosition(newDocId, postingWriter);
    if (newDocId != INVALID_DOCID)
    {
        if (mFormatOption.HasFieldMap())
        {
            postingWriter->SetFieldMap(mFieldMapBuffer[mDocBufCursor]);
        }

        if (mFormatOption.HasTermFrequency() 
            && !mFormatOption.HasPositionList())
        {
            postingWriter->SetCurrentTF(tf);
        }

        if (mFormatOption.HasTermPayload())
        {
            assert(mDecoder);
            postingWriter->SetTermPayload(mDecoder->GetTermMeta()->GetPayload());
        } 

        postingWriter->EndDocument(newDocId, mDocPayloadBuf[mDocBufCursor]);
    }
    mDocBufCursor++;
}

tf_t OneDocMerger::MergePosition(docid_t newDocId,
                                 PostingWriterImpl* postingWriter)
{
    if (!mFormatOption.HasTermFrequency())
    {
        return 0;
    }

    tf_t tf = 0;
    pos_t pos = 0;
    while (true)
    {
        if (mFormatOption.HasPositionList())
        {
            if (mPosBufCursor == mPosCountInBuf)
            {
                mPosBufCursor = 0;
                mPosCountInBuf = mDecoder->DecodePosList(mPosBuf,
                        mPosPayloadBuf, MAX_POS_PER_RECORD);
                if (mPosCountInBuf == 0)
                {
                    break;
                }
            }
            if (newDocId != INVALID_DOCID)
            {
                pos += mPosBuf[mPosBufCursor];
                postingWriter->AddPosition(pos, mPosPayloadBuf[mPosBufCursor], 0);
            }
            mPosBufCursor++;
        }
        tf++;
        mPosIndex++;

        if (mFormatOption.HasTfList() && tf >= mTfListBuf[mDocBufCursor])
        {
            break;
        }

        if (mFormatOption.HasTfBitmap() && mDecoder->IsDocEnd(mPosIndex))
        {
            break;
        }
    }
    return tf;
}


IE_NAMESPACE_END(index);

