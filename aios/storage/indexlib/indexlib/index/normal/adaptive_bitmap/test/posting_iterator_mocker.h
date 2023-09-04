#ifndef __INDEXLIB_POSTING_ITERATOR_MOCKER_H
#define __INDEXLIB_POSTING_ITERATOR_MOCKER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class PostingIteratorMocker : public PostingIterator
{
public:
    PostingIteratorMocker(std::vector<docid_t>& docList);
    ~PostingIteratorMocker();

public:
    index::ErrorCode SeekDocWithErrorCode(docid_t docId, docid_t& result) override
    {
        result = SeekDoc(docId);
        return index::ErrorCode::OK;
    }
    docid_t SeekDoc(docid_t docId) override;
    TermMeta* GetTermMeta() const override { return NULL; }
    bool HasPosition() const override { return false; }
    void Unpack(TermMatchData& termMatchData) override {}
    void Reset() override { mCursor = 0; }
    PostingIterator* Clone() const override
    {
        assert(false);
        return NULL;
    }

private:
    std::vector<docid_t> mDocList;
    uint32_t mCursor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PostingIteratorMocker);
}} // namespace indexlib::index

#endif //__INDEXLIB_POSTING_ITERATOR_MOCKER_H
