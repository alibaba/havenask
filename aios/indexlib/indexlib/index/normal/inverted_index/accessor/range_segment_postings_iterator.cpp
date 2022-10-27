#include "indexlib/index/normal/inverted_index/accessor/range_segment_postings_iterator.h"
#include "indexlib/index/normal/inverted_index/format/dict_inline_posting_decoder.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/pair_value_skip_list_reader.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/tri_value_skip_list_reader.h"
#include "indexlib/index/normal/inverted_index/format/term_meta_loader.h"
#include "indexlib/index/normal/inverted_index/format/short_list_segment_decoder.h"
#include "indexlib/index/normal/inverted_index/format/skip_list_segment_decoder.h"
#include "indexlib/index/normal/inverted_index/format/in_mem_posting_decoder.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, RangeSegmentPostingsIterator);

RangeSegmentPostingsIterator::RangeSegmentPostingsIterator(
        const PostingFormatOption& postingFormatOption,
        uint32_t& seekDocCounter,
        mem_pool::Pool* sessionPool)
    : mSessionPool(sessionPool)
    , mSeekDocCounter(seekDocCounter)
    , mDocBuffer(NULL)
    , mCurrentDocId(INVALID_DOCID)
    , mBaseDocId(INVALID_DOCID)
    , mNextSegmentDocId(INVALID_DOCID)
    , mBufferLength(0)
{
}

void RangeSegmentPostingsIterator::Reset()
{
   while (!mHeap.empty())
   {
        mHeap.pop();
   }
   mCurrentDocId = INVALID_DOCID;
   mBaseDocId = INVALID_DOCID;
   mNextSegmentDocId = INVALID_DOCID;
   for (size_t i = 0; i < mSegmentDecoders.size(); i++)
   {
       if (mSegmentDecoders[i])
       {
           IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mSegmentDecoders[i]);
       }
   }
   IE_POOL_COMPATIBLE_DELETE_VECTOR(mSessionPool, mDocBuffer,
                                    MAX_DOC_PER_RECORD * mBufferLength);
   mBufferLength = 0;
   mDocListReaders.clear();
   mSegmentDecoders.clear();
   mSegPostings.reset();
}

RangeSegmentPostingsIterator::~RangeSegmentPostingsIterator() 
{
    for (size_t i = 0; i < mSegmentDecoders.size(); i++)
    {
        if (mSegmentDecoders[i])
        {
            IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mSegmentDecoders[i]);
        }
    }
    IE_POOL_COMPATIBLE_DELETE_VECTOR(mSessionPool, mDocBuffer,
                          MAX_DOC_PER_RECORD * mBufferLength);
}

void RangeSegmentPostingsIterator::Init(const SegmentPostingsPtr& segPostings,
                                       docid_t startDocId, docid_t nextSegmentDocId)
{
    std::vector<SegmentPosting>& postingVec = segPostings->GetSegmentPostings();
    size_t bufferLength = postingVec.size();
    bufferLength = std::max(bufferLength, (size_t)100);
    if (mDocBuffer && bufferLength > mBufferLength)
    {
        IE_POOL_COMPATIBLE_DELETE_VECTOR(mSessionPool, mDocBuffer,
                              MAX_DOC_PER_RECORD * mBufferLength);
        mDocBuffer = NULL;
    }
    if (!mDocBuffer)
    {
        mDocBuffer = IE_POOL_COMPATIBLE_NEW_VECTOR(mSessionPool, docid_t,
                MAX_DOC_PER_RECORD * bufferLength);
    }
    mBufferLength = bufferLength;
    mSegPostings = segPostings;
    while (!mHeap.empty())
    {
        mHeap.pop();
    }

    for (size_t i = 0; i < mSegmentDecoders.size(); i++)
    {
        if (mSegmentDecoders[i])
        {
            IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mSegmentDecoders[i]);
        }
    }

    mDocListReaders.resize(postingVec.size());
    mSegmentDecoders.resize(postingVec.size(), NULL);
    mBaseDocId = mSegPostings->GetBaseDocId();
    mNextSegmentDocId = nextSegmentDocId;
    docid_t curSegDocId = std::max(docid_t(0), startDocId - mBaseDocId);
    ttf_t currentTTF = 0;
    docid_t firstDocId = INVALID_DOCID;
    docid_t lastDocId = INVALID_DOCID;
    for (size_t i = 0; i < postingVec.size(); i ++)
    {
        mSegmentDecoders[i] = CreateSegmentDecoder(postingVec[i], &mDocListReaders[i]);
        if (!mSegmentDecoders[i]->DecodeDocBuffer(curSegDocId, mDocBuffer + i * MAX_DOC_PER_RECORD,
                        firstDocId, lastDocId, currentTTF))
        {
            continue;
        }
        mHeap.emplace(firstDocId, lastDocId, i * MAX_DOC_PER_RECORD);
    }
}

BufferedSegmentIndexDecoder* RangeSegmentPostingsIterator::CreateSegmentDecoder(
        const SegmentPosting& curSegPosting, common::ByteSliceReader* docListReaderPtr)
{
    index::PostingFormatOption curSegPostingFormatOption =
        curSegPosting.GetPostingFormatOption();
    uint8_t compressMode = curSegPosting.GetCompressMode();
    uint8_t docCompressMode = ShortListOptimizeUtil::GetDocCompressMode(compressMode);
    BufferedSegmentIndexDecoder* segmentDecoder = NULL;     
    if (docCompressMode == DICT_INLINE_COMPRESS_MODE)
    {
        segmentDecoder = IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool, 
                DictInlinePostingDecoder, curSegPostingFormatOption, 
                curSegPosting.GetDictInlinePostingData());
        return segmentDecoder;
    }

    const PostingWriter* postingWriter = curSegPosting.GetInMemPostingWriter();
    if (postingWriter)
    {
        InMemPostingDecoder* postingDecoder = 
            postingWriter->CreateInMemPostingDecoder(mSessionPool);
        segmentDecoder = postingDecoder->GetInMemDocListDecoder();
        IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, postingDecoder);
        return segmentDecoder;
    }

    ByteSliceList *postingList = curSegPosting.GetSliceListPtr().get();
    // do not use doclist reader to read, because reader can't seek back.
    docListReaderPtr->Open(postingList);

    index::TermMeta termMeta;
    index::TermMetaLoader tmLoader(curSegPostingFormatOption);
    common::ByteSliceReader docListReader;
    docListReader.Open(postingList);
    tmLoader.Load(&docListReader, termMeta);
    uint32_t docSkipListSize = docListReader.ReadVUInt32();
    docListReader.ReadVUInt32();//move doc list size
    uint32_t docListBeginPos = docListReader.Tell() + docSkipListSize;
    segmentDecoder = CreateNormalSegmentDecoder(
            docListReaderPtr, docCompressMode, docListBeginPos,
            curSegPostingFormatOption);

    uint32_t docSkipListStart = docListReader.Tell();
    uint32_t docSkipListEnd = docSkipListStart + docSkipListSize;
    segmentDecoder->InitSkipList(docSkipListStart, docSkipListEnd,
                                  postingList, termMeta.GetDocFreq(),
                                  curSegPostingFormatOption.GetDocListCompressMode());

    return segmentDecoder;
}

BufferedSegmentIndexDecoder* RangeSegmentPostingsIterator::CreateNormalSegmentDecoder(
        common::ByteSliceReader* docListReader, uint8_t compressMode,
        uint32_t docListBeginPos, index::PostingFormatOption& curSegPostingFormatOption)
{
    if (compressMode == SHORT_LIST_COMPRESS_MODE)
    {
        return IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool, ShortListSegmentDecoder,
                docListReader, docListBeginPos, compressMode);
    }

    if (curSegPostingFormatOption.HasTfList())
    {
        return IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool, 
                SkipListSegmentDecoder<TriValueSkipListReader>,
                mSessionPool, docListReader, docListBeginPos, compressMode);
    }
    else
    {
        return IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool, 
                SkipListSegmentDecoder<PairValueSkipListReader>,
                mSessionPool, docListReader, docListBeginPos, compressMode);
    }
}

IE_NAMESPACE_END(index);

