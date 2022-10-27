#ifndef __INDEXLIB_TRUNCATE_POSTING_ITERATOR_H
#define __INDEXLIB_TRUNCATE_POSTING_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"

IE_NAMESPACE_BEGIN(index);

class TruncatePostingIterator : public PostingIterator
{
public:
    TruncatePostingIterator(const PostingIteratorPtr &evaluatorIter,
                            const PostingIteratorPtr &collectorIter);
    ~TruncatePostingIterator();

public:
    index::TermMeta* GetTermMeta() const override;

    docid_t SeekDoc(docid_t docId) override;
    
    common::ErrorCode SeekDocWithErrorCode(docid_t docId, docid_t& result) override;    

    bool HasPosition() const override;

    common::ErrorCode SeekPositionWithErrorCode(pos_t pos, pos_t& nextpos) override;
    void Unpack(TermMatchData& termMatchData) override;

    docpayload_t GetDocPayload() override;

    PostingIteratorType GetType() const override;

    void Reset() override;

    PostingIterator* Clone() const override; 
    autil::mem_pool::Pool *GetSessionPool() const override {
        return NULL;
    }

public:
    PostingIteratorPtr GetCurrentIterator() const { return mCurIter; }

private:
    PostingIteratorPtr mEvaluatorIter;
    PostingIteratorPtr mCollectorIter;
    PostingIteratorPtr mCurIter;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncatePostingIterator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TRUNCATE_POSTING_ITERATOR_H
