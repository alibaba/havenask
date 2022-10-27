#ifndef __INDEXLIB_BUFFERED_POSTING_ITERATOR_H
#define __INDEXLIB_BUFFERED_POSTING_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/numeric_compress/reference_compress_int_encoder.h"
#include "indexlib/index/normal/inverted_index/accessor/term_match_data.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator_impl.h"
#include "indexlib/index/normal/inverted_index/accessor/section_attribute_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_posting.h"
#include "indexlib/index/normal/inverted_index/format/buffered_index_decoder.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_in_doc_state.h"

IE_NAMESPACE_BEGIN(index);
class BufferedPostingIterator : public PostingIteratorImpl
{
public:
    BufferedPostingIterator(const index::PostingFormatOption& postingFormatOption,
                            autil::mem_pool::Pool *sessionPool);

    virtual ~BufferedPostingIterator();

private:
    BufferedPostingIterator(const BufferedPostingIterator& other);

public:
    virtual bool Init(const SegmentPostingVectorPtr &segPostings,
                      const SectionAttributeReader* sectionReader, 
                      const uint32_t statePoolSize);
    
    docid_t SeekDoc(docid_t docId) override;
    common::ErrorCode SeekDocWithErrorCode(docid_t docId, docid_t& result) override;
    void Unpack(TermMatchData& termMatchData) override;

    docpayload_t GetDocPayload() override { return InnerGetDocPayload(); }
    pospayload_t GetPosPayload();
    PostingIteratorType GetType() const override { return pi_buffered; }
    common::ErrorCode SeekPositionWithErrorCode(pos_t pos, pos_t &nextPos) override;
    PostingIterator* Clone() const override;

    bool HasPosition() const override
    { return mPostingFormatOption.HasPositionList(); }
    void AllocateBuffers();

public:
    // for runtime inline.
    docid_t InnerSeekDoc(docid_t docId);
    common::ErrorCode InnerSeekDoc(docid_t docId, docid_t& result);
    fieldmap_t GetFieldMap();
    common::ErrorCode GetFieldMap(fieldmap_t& fieldMap);
    void Reset() override;

private:
    common::ErrorCode SeekDocForNormal(docid_t docId, docid_t& result);
    common::ErrorCode SeekDocForReference(docid_t docId, docid_t& result);    

private:
    uint32_t GetCurrentSeekedDocCount() const;
    docpayload_t InnerGetDocPayload();
    void DecodeTFBuffer();
    void DecodeDocPayloadBuffer();
    void DecodeFieldMapBuffer();
    int32_t GetDocOffsetInBuffer() const;
    tf_t InnerGetTF();
    fieldmap_t InnerGetFieldMap() const;
    ttf_t GetCurrentTTF();
    void MoveToCurrentDoc();

private:
    //virtual for test
    virtual BufferedPostingDecoder* CreateBufferedPostingDecoder();

protected:
    index::PostingFormatOption mPostingFormatOption;
    docid_t mLastDocIdInBuffer;
    docid_t mCurrentDocId;
    docid_t *mDocBufferCursor;
    docid_t mDocBuffer[MAX_DOC_PER_RECORD];
    docid_t *mDocBufferBase;
    ttf_t mCurrentTTF;
    int32_t mTFBufferCursor;
    tf_t *mTFBuffer;
    docpayload_t *mDocPayloadBuffer;
    fieldmap_t *mFieldMapBuffer;

    BufferedPostingDecoder *mDecoder;

    common::ReferenceCompressIntReader<uint32_t> mRefCompressReader;
 
    bool mNeedMoveToCurrentDoc;
    bool mInDocPosIterInited;
    NormalInDocPositionIterator *mInDocPositionIterator;
    NormalInDocState mState;
    util::ObjectPool<NormalInDocState> mStatePool;

private:
    friend class BufferedPostingIteratorTest;
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<BufferedPostingIterator> BufferedPostingIteratorPtr;

//////////////////////////////////////////////////////////////////////////////
//inline functions

inline docpayload_t BufferedPostingIterator::InnerGetDocPayload()
{
    if (mPostingFormatOption.HasDocPayload())
    {
        DecodeTFBuffer();
        DecodeDocPayloadBuffer();
        return mDocPayloadBuffer[GetDocOffsetInBuffer()];
    }
    return 0;
}

inline fieldmap_t BufferedPostingIterator::GetFieldMap()
{
    fieldmap_t fieldMap = fieldmap_t();
    auto ec = GetFieldMap(fieldMap);
    common::ThrowIfError(ec);
    return fieldMap;
}


inline tf_t BufferedPostingIterator::InnerGetTF()
{
    if (mPostingFormatOption.HasTfList())
    {
        DecodeTFBuffer();
        return mTFBuffer[GetDocOffsetInBuffer()];
    }
    return 0;
}

inline ttf_t BufferedPostingIterator::GetCurrentTTF()
{
    if (mPostingFormatOption.HasTfList())
    {
        DecodeTFBuffer();
        int32_t offset = GetDocOffsetInBuffer(); // get ttf before this doc.
        for (; mTFBufferCursor < offset; ++mTFBufferCursor)
        {
            mCurrentTTF += mTFBuffer[mTFBufferCursor];
        }
    }
    return mCurrentTTF;
}

inline void BufferedPostingIterator::DecodeTFBuffer()
{
    if (mPostingFormatOption.HasTfList())
    {
        if (mDecoder->DecodeCurrentTFBuffer(mTFBuffer))
        {
            mTFBufferCursor = 0;
        }
    }
}

inline void BufferedPostingIterator::DecodeDocPayloadBuffer()
{
    if (mPostingFormatOption.HasDocPayload())
    {
        mDecoder->DecodeCurrentDocPayloadBuffer(mDocPayloadBuffer);
    }
}

inline void BufferedPostingIterator::DecodeFieldMapBuffer()
{
    if (mPostingFormatOption.HasFieldMap())
    {
        mDecoder->DecodeCurrentFieldMapBuffer(mFieldMapBuffer);
    }
}

inline void BufferedPostingIterator::MoveToCurrentDoc()
{
    mNeedMoveToCurrentDoc = false;
    mInDocPosIterInited = false;
    if (mPostingFormatOption.HasTermFrequency())
    {
        mState.SetDocId(mCurrentDocId);
        mState.SetTermFreq(InnerGetTF());

        uint32_t seekedDocCount = GetCurrentSeekedDocCount();
        mState.SetSeekedDocCount(seekedDocCount);

        if (mPostingFormatOption.HasPositionList())
        {
            ttf_t currentTTF = 0;
            currentTTF = GetCurrentTTF();
            mDecoder->MoveToCurrentDocPosition(currentTTF);
        }
    }
}

inline int32_t BufferedPostingIterator::GetDocOffsetInBuffer() const
{
    if (mPostingFormatOption.IsReferenceCompress()) 
    {
        return mRefCompressReader.GetCursor() - 1;
    }
    return mDocBufferCursor - mDocBufferBase - 1;
}

inline fieldmap_t BufferedPostingIterator::InnerGetFieldMap() const
{
    return mFieldMapBuffer[GetDocOffsetInBuffer()];
}

// TODO: opt with using ReferencePostingIterator instead of if
inline docid_t BufferedPostingIterator::InnerSeekDoc(docid_t docId)
{
    docid_t ret = INVALID_DOCID;
    auto ec = InnerSeekDoc(docId, ret);
    common::ThrowIfError(ec);
    return ret;
}
    
inline common::ErrorCode BufferedPostingIterator::InnerSeekDoc(docid_t docId, docid_t& result)
{
    if (mPostingFormatOption.IsReferenceCompress()) 
    {
        return SeekDocForReference(docId, result);
    }
    return SeekDocForNormal(docId, result);
}


inline common::ErrorCode BufferedPostingIterator::SeekDocForNormal(docid_t docId, docid_t& result)
{
    docid_t curDocId = mCurrentDocId;
    docId = std::max(curDocId+1, docId);
    if (unlikely(docId > mLastDocIdInBuffer))
    {
        try
        {
            if (!mDecoder->DecodeDocBuffer(docId, mDocBuffer, curDocId,
                                           mLastDocIdInBuffer, mCurrentTTF))
            {
                result = INVALID_DOCID;
                return common::ErrorCode::OK;
            }
            mDocBufferCursor = mDocBuffer + 1;
            mPostingFormatOption = mDecoder->GetPostingFormatOption();
        }
        catch (const misc::FileIOException& e)
        {
            return common::ErrorCode::FileIO;
        }
    }
    docid_t *cursor = mDocBufferCursor;
    while (curDocId < docId)
    {
        curDocId += *(cursor++);
    }
    mCurrentDocId = curDocId;
    mDocBufferCursor = cursor;
    mNeedMoveToCurrentDoc = true;
    result = curDocId;
    return common::ErrorCode::OK;
}

inline common::ErrorCode BufferedPostingIterator::SeekDocForReference(docid_t docId, docid_t& result)
{
    docid_t curDocId = mCurrentDocId;
    if (unlikely(curDocId + 1 > docId))
    {
        docId = curDocId+1;
    }
    if (unlikely(docId > mLastDocIdInBuffer))
    {
        mDocBufferBase = mDocBuffer; // copy when use block file only
        try
        {
            if (!mDecoder->DecodeDocBufferMayCopy(docId, mDocBufferBase, curDocId,
                                                  mLastDocIdInBuffer, mCurrentTTF))
            {
                result = INVALID_DOCID;
                return common::ErrorCode::OK;
            }
            mPostingFormatOption = mDecoder->GetPostingFormatOption();
            mRefCompressReader.Reset((char*)mDocBufferBase);
        }
        catch (const misc::FileIOException& e)
        {
            return common::ErrorCode::FileIO;
        }        
    }
    //TODO: optimze
    if (curDocId < docId)
    {
        docid_t baseDocId = mDecoder->GetCurrentSegmentBaseDocId();
        assert(docId > baseDocId);
        curDocId = mRefCompressReader.Seek(docId - baseDocId);
        curDocId += baseDocId;
    }
    mCurrentDocId = curDocId;
    mNeedMoveToCurrentDoc = true;
    result = curDocId;
    return common::ErrorCode::OK;
}

inline common::ErrorCode BufferedPostingIterator::GetFieldMap(fieldmap_t &fieldMap)
{
    if (mPostingFormatOption.HasFieldMap())
    {
        try
        {
            DecodeTFBuffer();
            DecodeDocPayloadBuffer();
            DecodeFieldMapBuffer();
            fieldMap = InnerGetFieldMap();
            return common::ErrorCode::OK;
        }
        catch (const misc::FileIOException& e)
        {
            return common::ErrorCode::FileIO;
        }
    }
    fieldMap = fieldmap_t();
    return common::ErrorCode::OK;    
}

inline void BufferedPostingIterator::AllocateBuffers()
{
    mTFBuffer = IE_POOL_COMPATIBLE_NEW_VECTOR(mSessionPool,
            tf_t, MAX_DOC_PER_RECORD);
    mDocPayloadBuffer = IE_POOL_COMPATIBLE_NEW_VECTOR(mSessionPool, 
            docpayload_t, MAX_DOC_PER_RECORD);
    mFieldMapBuffer = IE_POOL_COMPATIBLE_NEW_VECTOR(mSessionPool,
            fieldmap_t, MAX_DOC_PER_RECORD);
}

inline uint32_t BufferedPostingIterator::GetCurrentSeekedDocCount() const
{
    return mDecoder->InnerGetSeekedDocCount() + (GetDocOffsetInBuffer() + 1);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BUFFERED_POSTING_ITERATOR_H
