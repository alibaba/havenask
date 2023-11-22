#pragma once

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
    index::ErrorCode SeekDocWithErrorCode(docid64_t docId, docid64_t& result) override
    {
        result = SeekDoc(docId);
        return index::ErrorCode::OK;
    }
    docid64_t SeekDoc(docid64_t docId) override;
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
