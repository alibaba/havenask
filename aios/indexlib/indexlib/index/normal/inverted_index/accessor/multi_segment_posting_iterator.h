#ifndef __INDEXLIB_MULTI_SEGMENT_POSTING_ITERATOR_H
#define __INDEXLIB_MULTI_SEGMENT_POSTING_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/in_mem_file_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator_impl.h"
IE_NAMESPACE_BEGIN(index);

class MultiSegmentPostingIterator : public PostingIterator
{
public:
    typedef std::pair<int64_t, PostingIteratorPtr> PostingIteratorInfo;
public:
    MultiSegmentPostingIterator();
    ~MultiSegmentPostingIterator();
public:
    void Init(std::vector<PostingIteratorInfo> postingIterators,
              std::vector<segmentid_t> segmentIdxs,
              std::vector<common::InMemFileWriterPtr> fileWriters);

public:
    size_t GetSegmentPostingCount() const { return mSize; }
    PostingIteratorPtr GetSegmentPostingIter(size_t idx, segmentid_t& segmentIdx)
    {
        segmentIdx = mSegmentIdxs[idx];
        return mPostingIterators[idx].second;
    }

    index::TermMeta* GetTermMeta() const override;
    index::TermMeta *GetTruncateTermMeta() const override;
    index::TermPostingInfo GetTermPostingInfo() const override;
    autil::mem_pool::Pool *GetSessionPool() const override;
    
    docid_t SeekDoc(docid_t docId) override;
    common::ErrorCode SeekDocWithErrorCode(docid_t docId, docid_t& result) override;

    bool HasPosition() const override { return mPostingIterators[mCursor].second->HasPosition(); }
    void Unpack(TermMatchData& termMatchData) override { mPostingIterators[mCursor].second->Unpack(termMatchData); }

    docpayload_t GetDocPayload() override
    {
        return mPostingIterators[mCursor].second->GetDocPayload();
    }

    PostingIteratorType GetType() const override
    {
        assert(false);
        return pi_unknown;
    }

    void Reset() override { 
        for(auto& postingIterator : mPostingIterators)
        {
            postingIterator.second->Reset();
        }
        mCursor = 0;
    }

    PostingIterator* Clone() const override
    {
        MultiSegmentPostingIterator* multiSegmentPostingIterator
            = new MultiSegmentPostingIterator();
        std::vector<PostingIteratorInfo> postingIters;
        for (auto& postingIter : mPostingIterators)
        {
            PostingIteratorPtr postingIterClone = PostingIteratorPtr(postingIter.second->Clone());
            postingIters.push_back(std::make_pair(postingIter.first, postingIterClone));
        }
        multiSegmentPostingIterator->Init(postingIters, mSegmentIdxs, mFileWriters);
        return multiSegmentPostingIterator;
    }

    matchvalue_t GetMatchValue() const override
    {
        assert(false);
        return matchvalue_t();
    }

    termpayload_t GetTermPayLoad() 
    {
        if (mPostingIterators[mCursor].second->GetTermMeta())
        {
            return mPostingIterators[mCursor].second->GetTermMeta()->GetPayload();
        }
        return -1;
    }

private:
    std::vector<PostingIteratorInfo> mPostingIterators;
    std::vector<common::InMemFileWriterPtr> mFileWriters;
    std::vector<segmentid_t> mSegmentIdxs;
    int64_t mCursor;
    int64_t mSize;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiSegmentPostingIterator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTI_SEGMENT_POSTING_ITERATOR_H
