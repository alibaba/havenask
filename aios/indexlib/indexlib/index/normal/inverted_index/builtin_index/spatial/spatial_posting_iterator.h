#ifndef __INDEXLIB_SPATIAL_POSTING_ITERATOR_H
#define __INDEXLIB_SPATIAL_POSTING_ITERATOR_H

#include <tr1/memory>
#include <queue>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/buffered_posting_iterator.h"

IE_NAMESPACE_BEGIN(index);

class SpatialPostingIterator : public PostingIterator
{
public:
    SpatialPostingIterator(autil::mem_pool::Pool* sessionPool);
    ~SpatialPostingIterator();

public:
    void Init(const std::vector<BufferedPostingIterator*>& postingIterators);

    docid_t SeekDoc(docid_t docid) override;
    common::ErrorCode SeekDocWithErrorCode(docid_t docId, docid_t& result) override {
        return InnerSeekDoc(docId, result);
    }
    docid_t InnerSeekDoc(docid_t docId);
    common::ErrorCode InnerSeekDoc(docid_t docId, docid_t& result);
    bool HasPosition() const override { return false; }
    void Unpack(TermMatchData& termMatchData) override;
    void Reset() override;
    PostingIterator* Clone() const override;
    PostingIteratorType GetType() const override { return pi_spatial; }
    index::TermMeta *GetTermMeta() const override
    { return mTermMeta; }
    autil::mem_pool::Pool *GetSessionPool() const override
    { return mSessionPool; }
    uint32_t GetSeekDocCount() const { return mSeekDocCounter; }

private:
    typedef std::pair<BufferedPostingIterator*, docid_t> PostingIteratorPair;
    class PostingIteratorComparator
    {
    public:
        bool operator() (const PostingIteratorPair& left,
                         const PostingIteratorPair& right)
        { return left.second >= right.second; }
    };

    typedef std::priority_queue<PostingIteratorPair, 
                                std::vector<PostingIteratorPair>,
                                PostingIteratorComparator> PostingIteratorHeap;

private:
    docid_t GetNextDocId(PostingIteratorHeap& heap);
    bool IsValidDocId(docid_t basicDocid, docid_t currentDocid) const;

private:
    //TODO: spatial posting iterator direct manage segment posting
    docid_t mCurDocid;
    PostingIteratorHeap mHeap;
    std::vector<BufferedPostingIterator*> mPostingIterators;
    index::TermMeta* mTermMeta;
    autil::mem_pool::Pool *mSessionPool;
    uint32_t mSeekDocCounter;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SpatialPostingIterator);

//////////////////////////////////////////////////////////////
inline docid_t SpatialPostingIterator::InnerSeekDoc(docid_t docid)
{
    docid_t result = 0;
    auto ec = InnerSeekDoc(docid, result);
    ThrowIfError(ec);
    return result;
}

inline common::ErrorCode SpatialPostingIterator::InnerSeekDoc(docid_t docid, docid_t& result)
{
    docid = std::max(mCurDocid + 1, docid);
    docid_t nextDocid = 0;
    while (!mHeap.empty())
    {
        mSeekDocCounter ++;
        auto item = mHeap.top();
        if (item.second >= docid)
        {
            mCurDocid = item.second;
            result = mCurDocid;
            return common::ErrorCode::OK;
        }
        mHeap.pop();
        auto ec = item.first->InnerSeekDoc(docid, nextDocid);
        if (unlikely(ec != common::ErrorCode::OK))
        {
            return ec;
        }
        if (nextDocid != INVALID_DOCID)
        {
            item.second = nextDocid;
            mHeap.push(item);
        }
    }
    result = INVALID_DOCID;
    return common::ErrorCode::OK;
}

inline docid_t SpatialPostingIterator::SeekDoc(docid_t docid)
{
    //should not seek back
    return InnerSeekDoc(docid);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SPATIAL_POSTING_ITERATOR_H
