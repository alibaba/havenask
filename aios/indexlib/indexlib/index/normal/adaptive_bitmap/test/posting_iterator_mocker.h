#ifndef __INDEXLIB_POSTING_ITERATOR_MOCKER_H
#define __INDEXLIB_POSTING_ITERATOR_MOCKER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"

IE_NAMESPACE_BEGIN(index);

class PostingIteratorMocker : public PostingIterator
{
public:
    PostingIteratorMocker(std::vector<docid_t>& docList);
    ~PostingIteratorMocker();
public:
    common::ErrorCode SeekDocWithErrorCode(docid_t docId, docid_t& result) override {
        result = SeekDoc(docId);
        return common::ErrorCode::OK;
    }
    docid_t SeekDoc(docid_t docId);
    index::TermMeta *GetTermMeta() const
    { return NULL; }
    bool HasPosition() const override { return false; }
    void Unpack(TermMatchData& termMatchData) override {}
    void Reset() override { mCursor = 0; }
    PostingIterator* Clone() const override
    { assert(false); return NULL; }
    
private:
    std::vector<docid_t> mDocList;
    uint32_t mCursor;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PostingIteratorMocker);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSTING_ITERATOR_MOCKER_H
