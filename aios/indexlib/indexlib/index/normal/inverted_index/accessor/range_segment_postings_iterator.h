#ifndef __INDEXLIB_RANGE_SEGMENT_POSTINGS_ITERATOR_H
#define __INDEXLIB_RANGE_SEGMENT_POSTINGS_ITERATOR_H

#include <tr1/memory>
#include <queue>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"
#include "indexlib/index/normal/inverted_index/format/buffered_segment_index_decoder.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_posting.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_postings.h"
#include "indexlib/common/byte_slice_reader.h"
#include "indexlib/common/error_code.h"

IE_NAMESPACE_BEGIN(index);
class RangeSegmentPostingsIterator
{
public:
    RangeSegmentPostingsIterator(
            const index::PostingFormatOption& postingFormatOption,
            uint32_t& seekDocCounter,
            autil::mem_pool::Pool* sessionPool);
    ~RangeSegmentPostingsIterator();
private:
    struct PostingItem
    {
        docid_t docid;
        docid_t lastDocid;        
        uint32_t bufferCursor;
        PostingItem(docid_t did,
                    docid_t lastid,                     
                    uint32_t cursor)
            : docid(did)
            , lastDocid(lastid)              
            , bufferCursor(cursor)
        {
        }
    };
    class PostingItemComparator
    {
    public:
        bool operator() (const PostingItem& left,
                         const PostingItem& right)
        { return left.docid > right.docid; }
    };
    typedef std::priority_queue<PostingItem, std::vector<PostingItem>,
                                PostingItemComparator > RangeHeap;
public:
    void Init(const SegmentPostingsPtr& segPostings, docid_t startDocid,
              docid_t nextSegmentDocId);
    common::Result<bool> Seek(docid_t docid);
    docid_t GetCurrentDocid() const { return mCurrentDocId; }
    void Reset();
private:
    BufferedSegmentIndexDecoder* CreateSegmentDecoder(const SegmentPosting& segmentPosting,
            common::ByteSliceReader* docListReaderPtr);
    BufferedSegmentIndexDecoder* CreateNormalSegmentDecoder(
            common::ByteSliceReader* docListReader,
            uint8_t compressMode, uint32_t docListBeginPos,
            index::PostingFormatOption& curSegPostingFormatOption);
private:
    autil::mem_pool::Pool* mSessionPool;
    uint32_t &mSeekDocCounter;
    docid_t* mDocBuffer;    
    docid_t mCurrentDocId;
    docid_t mBaseDocId;
    docid_t mNextSegmentDocId;
    uint32_t mBufferLength;
    index::PostingFormatOption mPostingFormatOption;
    SegmentPostingsPtr mSegPostings;
    std::vector<BufferedSegmentIndexDecoder*> mSegmentDecoders;
    std::vector<common::ByteSliceReader> mDocListReaders;
    RangeHeap mHeap;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RangeSegmentPostingsIterator);
/////////////////////////////////////////////////////////
inline common::Result<bool> RangeSegmentPostingsIterator::Seek(docid_t docid)
{
    if (mNextSegmentDocId != INVALID_DOCID && docid >= mNextSegmentDocId)
    {
        return false;
    }
    docid_t innerDocId = docid - mBaseDocId;
    while(!mHeap.empty())
    {
        mSeekDocCounter++;
        const PostingItem& item = mHeap.top();
        if (item.docid >= innerDocId)
        {
            mCurrentDocId = item.docid + mBaseDocId;
            return true;
        }
    
        PostingItem tmpItem = item;
        mHeap.pop();
        ttf_t currentTTF;
        if (unlikely(innerDocId > tmpItem.lastDocid))
        {
            try{
                size_t bufferIdx = tmpItem.bufferCursor >> 7;
                if (!mSegmentDecoders[bufferIdx]->DecodeDocBuffer(
                        innerDocId, mDocBuffer + (bufferIdx << 7), tmpItem.docid,
                        tmpItem.lastDocid, currentTTF))
                {
                    continue;                
                }
                tmpItem.bufferCursor = (bufferIdx << 7);
            }
            catch (const misc::FileIOException& e)
            {
                return false;
            }
        }
        docid_t *cursor = mDocBuffer + tmpItem.bufferCursor;
        docid_t curDocId = tmpItem.docid;
        while (curDocId < innerDocId)
        {
            curDocId += *(++cursor);
        }
        tmpItem.bufferCursor = cursor - mDocBuffer;
        tmpItem.docid = curDocId;
        mHeap.push(tmpItem);
    }
    return false;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_RANGE_SEGMENT_POSTINGS_ITERATOR_H
