#ifndef __INDEXLIB_BUFFERED_INDEX_DECODER_H
#define __INDEXLIB_BUFFERED_INDEX_DECODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_posting.h"
#include "indexlib/index/normal/inverted_index/accessor/in_doc_state_keeper.h"
#include "indexlib/common/byte_slice_reader.h"
#include "indexlib/common/numeric_compress/encoder_provider.h"
#include "indexlib/index/normal/inverted_index/format/buffered_segment_index_decoder.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_in_doc_position_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer_impl.h"

IE_NAMESPACE_BEGIN(index);

class BufferedIndexDecoder
{
public:
    BufferedIndexDecoder(NormalInDocState *state, autil::mem_pool::Pool *pool);

    virtual ~BufferedIndexDecoder();
public:
    void Init(const SegmentPostingVectorPtr& segPostings);
public:
    virtual bool DecodeDocBuffer(docid_t startDocId, docid_t *docBuffer,
                                 docid_t &firstDocId, docid_t &lastDocId,
                                 ttf_t &currentTTF);

    virtual bool DecodeDocBufferMayCopy(docid_t startDocId, docid_t *&docBuffer,
            docid_t &firstDocId, docid_t &lastDocId, ttf_t &currentTTF);
    
    virtual bool DecodeCurrentTFBuffer(tf_t *tfBuffer);
    virtual void DecodeCurrentDocPayloadBuffer(docpayload_t *docPayloadBuffer);
    virtual void DecodeCurrentFieldMapBuffer(fieldmap_t *fieldBitmapBuffer);

    virtual uint32_t GetSeekedDocCount() const;
    uint32_t InnerGetSeekedDocCount() const
    {  return mSegmentDecoder->InnerGetSeekedDocCount(); }

    void MoveToCurrentDocPosition(ttf_t currentTTF);
    NormalInDocPositionIterator* GetPositionIterator();

    const index::PostingFormatOption& GetPostingFormatOption() const 
    { return mCurSegPostingFormatOption; }
    docid_t GetCurrentSegmentBaseDocId() { return mBaseDocId; }

private:
    bool DecodeDocBufferInOneSegment(docid_t startDocId, docid_t *docBuffer,
            docid_t &firstDocId, docid_t &lastDocId, ttf_t &currentTTF);
    bool DecodeDocBufferInOneSegmentMayCopy(docid_t startDocId, docid_t *&docBuffer,
            docid_t &firstDocId, docid_t &lastDocId, ttf_t &currentTTF);
    
    bool DecodeShortListDocBuffer(docid_t startDocId, docid_t *docBuffer,
                                  docid_t &firstDocId, docid_t &lastDocId,
                                  ttf_t &currentTTF);

    bool DecodeNormalListDocBuffer(docid_t startDocId, docid_t *docBuffer,
                                   docid_t &firstDocId, docid_t &lastDocId,
                                   ttf_t &currentTTF);

private:
    // virtual for test
    virtual BufferedSegmentIndexDecoder *CreateNormalSegmentDecoder(
            uint8_t compressMode, uint32_t docListBeginPos);
private:
    bool MoveToSegment(docid_t startDocId);
    docid_t GetSegmentBaseDocId(uint32_t segCursor);
    uint32_t LocateSegment(uint32_t startSegCursor, docid_t startDocId);

protected:
    index::PostingFormatOption mCurSegPostingFormatOption;
private:
    docid_t mBaseDocId;

    bool mNeedDecodeTF;
    bool mNeedDecodeDocPayload;
    bool mNeedDecodeFieldMap;

    BufferedSegmentIndexDecoder *mSegmentDecoder;

    uint32_t mSegmentCursor;
    SegmentPostingVectorPtr mSegPostings;

    // PERF_OPT: reentry for multi segment decoder
    common::ByteSliceReader mDocListReader;

    autil::mem_pool::Pool *mSessionPool;
    NormalInDocPositionIterator *mInDocPositionIterator;
    InDocStateKeeper mInDocStateKeeper;
private:
    friend class BufferedIndexDecoderTest;
private:
    IE_LOG_DECLARE();
};

inline void BufferedIndexDecoder::MoveToCurrentDocPosition(ttf_t currentTTF)
{
    mInDocStateKeeper.MoveToDoc(currentTTF);
}

inline NormalInDocPositionIterator* BufferedIndexDecoder::GetPositionIterator()
{
    return mInDocPositionIterator;
}

inline docid_t BufferedIndexDecoder::GetSegmentBaseDocId(uint32_t segCursor)
{
    if ((size_t)segCursor >= mSegPostings->size())
    {
        return INVALID_DOCID;
    }

    SegmentPosting& curSegPosting = (*mSegPostings)[segCursor];
    return curSegPosting.GetBaseDocId();
}

inline uint32_t BufferedIndexDecoder::LocateSegment(
        uint32_t startSegCursor, docid_t startDocId)
{
    docid_t curSegBaseDocId = GetSegmentBaseDocId(startSegCursor);
    if (curSegBaseDocId == INVALID_DOCID)
    {
        return startSegCursor;
    }

    uint32_t curSegCursor = startSegCursor;
    docid_t nextSegBaseDocId = GetSegmentBaseDocId(curSegCursor + 1);
    while (nextSegBaseDocId != INVALID_DOCID && startDocId >= nextSegBaseDocId) 
    {
        ++curSegCursor;
        nextSegBaseDocId = GetSegmentBaseDocId(curSegCursor + 1);
    }
    return curSegCursor;
}


inline uint32_t BufferedIndexDecoder::GetSeekedDocCount() const
{
    return InnerGetSeekedDocCount();
}

DEFINE_SHARED_PTR(BufferedIndexDecoder);

typedef BufferedIndexDecoder BufferedPostingDecoder;
typedef BufferedIndexDecoderPtr BufferedPostingDecoderPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BUFFERED_INDEX_DECODER_H
