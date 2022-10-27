#include "indexlib/index/normal/inverted_index/accessor/buffered_posting_iterator.h"
#include "indexlib/index/normal/attribute/accessor/section_attribute_reader_impl.h"

using namespace std;



IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BufferedPostingIterator);

BufferedPostingIterator::BufferedPostingIterator(const PostingFormatOption& postingFormatOption,
        autil::mem_pool::Pool *sessionPool) 
    : PostingIteratorImpl(sessionPool)
    , mPostingFormatOption(postingFormatOption)
    , mLastDocIdInBuffer(INVALID_DOCID - 1)
    , mCurrentDocId(INVALID_DOCID)
    , mDocBufferCursor(NULL)
    , mCurrentTTF(0)
    , mTFBufferCursor(0)
    , mTFBuffer(NULL)
    , mDocPayloadBuffer(NULL)
    , mFieldMapBuffer(NULL)
    , mDecoder(NULL)
    , mNeedMoveToCurrentDoc(false)
    , mInDocPosIterInited(false)
    , mInDocPositionIterator(NULL)
    , mState(0, postingFormatOption.GetPosListFormatOption())
{
    AllocateBuffers();
    mDocBufferBase = mDocBuffer;
}

BufferedPostingIterator::~BufferedPostingIterator() 
{
    IE_POOL_COMPATIBLE_DELETE_VECTOR(mSessionPool, mTFBuffer, MAX_DOC_PER_RECORD);
    IE_POOL_COMPATIBLE_DELETE_VECTOR(mSessionPool, mDocPayloadBuffer, MAX_DOC_PER_RECORD);
    IE_POOL_COMPATIBLE_DELETE_VECTOR(mSessionPool, mFieldMapBuffer, MAX_DOC_PER_RECORD);
    IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mDecoder);
}

bool BufferedPostingIterator::Init(const SegmentPostingVectorPtr& segPostings,
                                   const SectionAttributeReader* sectionReader, 
                                   const uint32_t statePoolSize)
{
    if (!PostingIteratorImpl::Init(segPostings))
    {
        return false;
    }
    mDecoder = CreateBufferedPostingDecoder();
    mDecoder->Init(segPostings);
    mStatePool.Init(statePoolSize);
    assert(dynamic_cast<const SectionAttributeReaderImpl*>(sectionReader) == sectionReader);
    mState.SetSectionReader(sectionReader);
    return true;
}

BufferedPostingDecoder* BufferedPostingIterator::CreateBufferedPostingDecoder()
{
    return IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool, BufferedPostingDecoder,
            &mState, mSessionPool);
}

docid_t BufferedPostingIterator::SeekDoc(docid_t docId)
{
    docid_t ret = INVALID_DOCID;
    auto ec = InnerSeekDoc(docId, ret);
    common::ThrowIfError(ec);
    return ret;
}

common::ErrorCode BufferedPostingIterator::SeekDocWithErrorCode(docid_t docId, docid_t& result)
{
    return InnerSeekDoc(docId, result);
}

void BufferedPostingIterator::Unpack(TermMatchData& termMatchData)
{
    DecodeTFBuffer();
    DecodeDocPayloadBuffer();
    DecodeFieldMapBuffer();
    if (mPostingFormatOption.HasDocPayload())
    {
        termMatchData.SetHasDocPayload(true);
        termMatchData.SetDocPayload(InnerGetDocPayload());
    }
    if (mPostingFormatOption.HasFieldMap())
    {
        termMatchData.SetFieldMap(InnerGetFieldMap());
    }
    if (mNeedMoveToCurrentDoc)
    {
        MoveToCurrentDoc();
    }
    if (mPostingFormatOption.HasTermFrequency()) 
    {
        NormalInDocState* state = mStatePool.Alloc();
        mState.Clone(state);
        termMatchData.SetInDocPositionState(state);
    }
    else
    {
        termMatchData.SetMatched(true);
    }
}

common::ErrorCode BufferedPostingIterator::SeekPositionWithErrorCode(pos_t pos, pos_t& result)    
{
    if (mNeedMoveToCurrentDoc)
    {
        try
        {
            MoveToCurrentDoc();
        }
        catch (const misc::FileIOException& e)
        {
            return common::ErrorCode::FileIO;
        }
    }

    if (!mInDocPosIterInited)
    {
        if (mPostingFormatOption.HasPositionList())
        {
            mInDocPosIterInited = true;
            mInDocPositionIterator = mDecoder->GetPositionIterator();
            try
            {
                mInDocPositionIterator->Init(mState);
            }
            catch (const misc::FileIOException& e)
            {
                return common::ErrorCode::FileIO;
            }
        }
        else
        {
            result = INVALID_POSITION;
            return common::ErrorCode::OK;
        }
    }
    return mInDocPositionIterator->SeekPositionWithErrorCode(pos, result);
}

pospayload_t BufferedPostingIterator::GetPosPayload()
{
    assert(mInDocPositionIterator);
    return mInDocPositionIterator->GetPosPayload();
}

PostingIterator* BufferedPostingIterator::Clone() const
{
    BufferedPostingIterator* iter = IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool,
            BufferedPostingIterator, mPostingFormatOption, mSessionPool);
    assert(iter != NULL);
    // in block cache mode: BlockByteSliceList already have first few block handles (by Lookup),
    //  this following Init have no file io inside, shuold not throw FileIOException;
    if (!iter->Init(mSegmentPostings, mState.GetSectionReader(), 
                    mStatePool.GetElemCount()))
    {
        IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, iter);
        iter = NULL;
    }
    return iter;
}

void BufferedPostingIterator::Reset()
{
    if (!mSegmentPostings || mSegmentPostings->size() == 0)
    {
        return;
    }

    IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mDecoder);
    mDecoder = CreateBufferedPostingDecoder();
    mDecoder->Init(mSegmentPostings);

    mCurrentDocId = INVALID_DOCID;
    mLastDocIdInBuffer = INVALID_DOCID - 1;
    mNeedMoveToCurrentDoc = false;
    mInDocPosIterInited = false;
}

IE_NAMESPACE_END(index);

