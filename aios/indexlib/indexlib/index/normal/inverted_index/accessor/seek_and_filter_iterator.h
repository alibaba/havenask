#ifndef __INDEXLIB_SEEK_AND_FILTER_ITERATOR_H
#define __INDEXLIB_SEEK_AND_FILTER_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/range_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/doc_value_filter.h"

IE_NAMESPACE_BEGIN(index);

class SeekAndFilterIterator  : public PostingIterator
{
public:
    SeekAndFilterIterator(PostingIterator* indexSeekIterator,
                          DocValueFilter* docFilter,
                          autil::mem_pool::Pool *sessionPool);
    ~SeekAndFilterIterator();
    
public:
    PostingIteratorType GetType() const override { return pi_seek_and_filter; }
    PostingIteratorType GetInnerIteratorType() const
    { return mIndexSeekIterator->GetType(); }

    DocValueFilter* GetDocValueFilter() const { return mDocFilter; }
    PostingIterator* GetIndexIterator() const { return mIndexSeekIterator; }

private:
    index::TermMeta *GetTermMeta() const override
    { return mIndexSeekIterator->GetTermMeta(); }
    index::TermMeta *GetTruncateTermMeta() const override
    { return mIndexSeekIterator->GetTruncateTermMeta(); }
    index::TermPostingInfo GetTermPostingInfo() const override
    { return mIndexSeekIterator->GetTermPostingInfo(); }
    autil::mem_pool::Pool *GetSessionPool() const override
    { return mSessionPool; }
    
    docid_t SeekDoc(docid_t docid) override;
    common::ErrorCode SeekDocWithErrorCode(docid_t docId, docid_t& result) override;
    docpayload_t GetDocPayload() override
    { return mIndexSeekIterator->GetDocPayload(); }
    bool HasPosition() const override
    { return mIndexSeekIterator->HasPosition(); }
    //TODO: support
    void Unpack(TermMatchData& termMatchData) override
    { mIndexSeekIterator->Unpack(termMatchData); }
    PostingIterator* Clone() const override;
    void Reset() override
    { mIndexSeekIterator->Reset(); }

private:
    PostingIterator* mIndexSeekIterator;
    DocValueFilter* mDocFilter;
    autil::mem_pool::Pool *mSessionPool;
    bool mNeedInnerFilter;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SeekAndFilterIterator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SEEK_AND_FILTER_ITERATOR_H
