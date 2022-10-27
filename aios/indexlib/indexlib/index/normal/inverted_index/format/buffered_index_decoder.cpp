#include "indexlib/index/normal/inverted_index/format/buffered_index_decoder.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"
#include "indexlib/index/normal/inverted_index/format/skip_list_segment_decoder.h"
#include "indexlib/index/normal/inverted_index/format/short_list_segment_decoder.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/pair_value_skip_list_reader.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/tri_value_skip_list_reader.h"
#include "indexlib/index/normal/inverted_index/format/dict_inline_posting_decoder.h"
#include "indexlib/index/normal/inverted_index/format/term_meta_loader.h"

using namespace std;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(common);

IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BufferedIndexDecoder);

BufferedIndexDecoder::BufferedIndexDecoder(NormalInDocState *state, Pool *pool) 
    : mBaseDocId(0)
    , mNeedDecodeTF(false)
    , mNeedDecodeDocPayload(false)
    , mNeedDecodeFieldMap(false)
    , mSegmentDecoder(NULL)
    , mSegmentCursor(0)
    , mSessionPool(pool)
    , mInDocPositionIterator(NULL)
    , mInDocStateKeeper(state, pool)
{
}

BufferedIndexDecoder::~BufferedIndexDecoder() 
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mSegmentDecoder);
    IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mInDocPositionIterator);
}

void BufferedIndexDecoder::Init(const SegmentPostingVectorPtr& segPostings)
{
    assert(segPostings);
    mSegPostings = segPostings;
    MoveToSegment(INVALID_DOCID);
}

bool BufferedIndexDecoder::DecodeDocBuffer(docid_t startDocId, docid_t *docBuffer,
        docid_t &firstDocId, docid_t &lastDocId, ttf_t &currentTTF)
{
    while (true)
    {
        if (DecodeDocBufferInOneSegment(startDocId, 
                        docBuffer, firstDocId, lastDocId, currentTTF))
        {
            return true;
        }
        if (!MoveToSegment(startDocId))
        {
            return false;
        }
    }
    return false;
}

bool BufferedIndexDecoder::DecodeDocBufferMayCopy(docid_t startDocId, 
        docid_t *&docBuffer, docid_t &firstDocId, docid_t &lastDocId, ttf_t &currentTTF)
{
    while (true)
    {
        if (DecodeDocBufferInOneSegmentMayCopy(startDocId, 
                        docBuffer, firstDocId, lastDocId, currentTTF))
        {
            return true;
        }
        if (!MoveToSegment(startDocId))
        {
            return false;
        }
    }
    return false;
}

bool BufferedIndexDecoder::DecodeCurrentTFBuffer(tf_t *tfBuffer)
{
    if (mNeedDecodeTF)
    {
        mSegmentDecoder->DecodeCurrentTFBuffer(tfBuffer);
        mNeedDecodeTF = false;
        return true;
    }
    return false;
}

void BufferedIndexDecoder::DecodeCurrentDocPayloadBuffer(
        docpayload_t *docPayloadBuffer)
{
    if (mNeedDecodeDocPayload)
    {
        mSegmentDecoder->DecodeCurrentDocPayloadBuffer(docPayloadBuffer);
        mNeedDecodeDocPayload = false;
    }
}

void BufferedIndexDecoder::DecodeCurrentFieldMapBuffer(fieldmap_t *fieldBitmapBuffer)
{
    if (!mNeedDecodeFieldMap)
    {
        return;
    }
    mSegmentDecoder->DecodeCurrentFieldMapBuffer(fieldBitmapBuffer);
    mNeedDecodeFieldMap = false;
}

bool BufferedIndexDecoder::DecodeDocBufferInOneSegment(docid_t startDocId,
        docid_t *docBuffer, docid_t &firstDocId, docid_t &lastDocId, ttf_t &currentTTF)
{
    docid_t nextSegBaseDocId = GetSegmentBaseDocId(mSegmentCursor);
    if (nextSegBaseDocId != INVALID_DOCID && startDocId >= nextSegBaseDocId)
    {
        // start docid not in current segment
        return false;
    }

    docid_t curSegDocId = max(docid_t(0), startDocId - mBaseDocId);
    if (!mSegmentDecoder->DecodeDocBuffer(curSegDocId, docBuffer, 
                    firstDocId, lastDocId, currentTTF))
    {
        return false;
    }
    mNeedDecodeTF = mCurSegPostingFormatOption.HasTfList();
    mNeedDecodeDocPayload = mCurSegPostingFormatOption.HasDocPayload();
    mNeedDecodeFieldMap = mCurSegPostingFormatOption.HasFieldMap();

    firstDocId += mBaseDocId;
    lastDocId += mBaseDocId;
    return true;
}

bool BufferedIndexDecoder::DecodeDocBufferInOneSegmentMayCopy(docid_t startDocId,
        docid_t *&docBuffer, docid_t &firstDocId, docid_t &lastDocId, ttf_t &currentTTF)
{
    docid_t nextSegBaseDocId = GetSegmentBaseDocId(mSegmentCursor);
    if (nextSegBaseDocId != INVALID_DOCID && startDocId >= nextSegBaseDocId)
    {
        // start docid not in current segment
        return false;
    }

    docid_t curSegDocId = max(docid_t(0), startDocId - mBaseDocId);
    if (!mSegmentDecoder->DecodeDocBufferMayCopy(curSegDocId, docBuffer, 
                    firstDocId, lastDocId, currentTTF))
    {
        return false;
    }
    mNeedDecodeTF = mCurSegPostingFormatOption.HasTfList();
    mNeedDecodeDocPayload = mCurSegPostingFormatOption.HasDocPayload();
    mNeedDecodeFieldMap = mCurSegPostingFormatOption.HasFieldMap();

    firstDocId += mBaseDocId;
    lastDocId += mBaseDocId;
    return true;
}

bool BufferedIndexDecoder::MoveToSegment(docid_t startDocId)
{
    uint32_t locateSegCursor = LocateSegment(mSegmentCursor, startDocId);
    if ((size_t)locateSegCursor >= mSegPostings->size())
    {
        return false;
    }

    mSegmentCursor = locateSegCursor;
    SegmentPosting& curSegPosting = (*mSegPostings)[mSegmentCursor];
    mCurSegPostingFormatOption = curSegPosting.GetPostingFormatOption();
    mBaseDocId = curSegPosting.GetBaseDocId();

    uint8_t compressMode = curSegPosting.GetCompressMode();
    uint8_t docCompressMode = ShortListOptimizeUtil::GetDocCompressMode(compressMode);
    if (docCompressMode == DICT_INLINE_COMPRESS_MODE)
    {
        IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mSegmentDecoder);
        mSegmentDecoder = IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool, 
                DictInlinePostingDecoder, mCurSegPostingFormatOption, 
                curSegPosting.GetDictInlinePostingData());

        ++mSegmentCursor;
        return true;
    }

    const PostingWriter* postingWriter = curSegPosting.GetInMemPostingWriter();
    if (postingWriter)
    {
        InMemPostingDecoder* postingDecoder = 
            postingWriter->CreateInMemPostingDecoder(mSessionPool);
        IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mSegmentDecoder);
        mSegmentDecoder = postingDecoder->GetInMemDocListDecoder();

        if (mCurSegPostingFormatOption.HasPositionList())
        {
            InMemPositionListDecoder* posDecoder =
                postingDecoder->GetInMemPositionListDecoder();
            assert(posDecoder);
            mInDocStateKeeper.MoveToSegment(posDecoder);
            IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mInDocPositionIterator);
            mInDocPositionIterator = IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool,
                    NormalInDocPositionIterator, 
                    mCurSegPostingFormatOption.GetPosListFormatOption());
        }
        IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, postingDecoder);
        ++mSegmentCursor;
        return true;
    }

    ByteSliceList *postingList = curSegPosting.GetSliceListPtr().get();
    // do not use doclist reader to read, because reader can't seek back.
    mDocListReader.Open(postingList);

    index::TermMeta termMeta;
    index::TermMetaLoader tmLoader(mCurSegPostingFormatOption);
    common::ByteSliceReader docListReader;
    docListReader.Open(postingList);
    tmLoader.Load(&docListReader, termMeta);
    uint32_t docSkipListSize = docListReader.ReadVUInt32();
    uint32_t docListSize = docListReader.ReadVUInt32();

    uint32_t docListBeginPos = docListReader.Tell() + docSkipListSize;
    IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mSegmentDecoder);
    mSegmentDecoder = CreateNormalSegmentDecoder(docCompressMode, docListBeginPos);

    uint32_t docSkipListStart = docListReader.Tell();
    uint32_t docSkipListEnd = docSkipListStart + docSkipListSize;
    mSegmentDecoder->InitSkipList(docSkipListStart, docSkipListEnd,
                                  postingList, termMeta.GetDocFreq(),
                                  mCurSegPostingFormatOption.GetDocListCompressMode());

    if (mCurSegPostingFormatOption.HasPositionList()
        || mCurSegPostingFormatOption.HasTfBitmap())
    {
        uint8_t posCompressMode =
            ShortListOptimizeUtil::GetPosCompressMode(compressMode);
        uint32_t posListBegin = docListReader.Tell() + docSkipListSize + docListSize;

        // init tf_bitmap_reader & position_list
        mInDocStateKeeper.MoveToSegment(postingList, termMeta.GetTotalTermFreq(),
                posListBegin, posCompressMode, 
                mCurSegPostingFormatOption.GetPosListFormatOption());
    }

    if (mCurSegPostingFormatOption.HasPositionList())
    {
        //InDocPositionIterator can't reentry in different segment
        IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mInDocPositionIterator);
        mInDocPositionIterator = IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool,
                NormalInDocPositionIterator,
                mCurSegPostingFormatOption.GetPosListFormatOption());
    }

    ++mSegmentCursor;
    return true;
}

BufferedSegmentIndexDecoder *BufferedIndexDecoder::CreateNormalSegmentDecoder(
        uint8_t compressMode, uint32_t docListBeginPos)
{
    if (compressMode == SHORT_LIST_COMPRESS_MODE)
    {
        return IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool, ShortListSegmentDecoder,
                &mDocListReader, docListBeginPos, compressMode);
    }

    if (mCurSegPostingFormatOption.HasTfList())
    {
        return IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool, 
                SkipListSegmentDecoder<TriValueSkipListReader>,
                mSessionPool, &mDocListReader, docListBeginPos, compressMode);
    }
    else
    {
        return IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool, 
                SkipListSegmentDecoder<PairValueSkipListReader>,
                mSessionPool, &mDocListReader, docListBeginPos, compressMode);
    }
}

IE_NAMESPACE_END(index);
