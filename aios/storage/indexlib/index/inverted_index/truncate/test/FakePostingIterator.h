#pragma once
#include "indexlib/index/inverted_index/PostingIterator.h"

namespace indexlib::index {

class FakePostingIterator : public PostingIterator
{
public:
    FakePostingIterator(const std::vector<docid32_t>& docIdVect) : _docIdVec(docIdVect), _cursor(-1), _termMeta(NULL)
    {
        _termMeta = new TermMeta;
        _termMeta->SetDocFreq(_docIdVec.size());
        for (int i = 0; i < docIdVect.size(); ++i) {
            _docPayload.push_back((docpayload_t)i);
        }
    }
    ~FakePostingIterator() { DELETE_AND_SET_NULL(_termMeta); }

    TermMeta* GetTermMeta() const override { return _termMeta; }

    PostingIterator* Clone() const override { return new FakePostingIterator(_docIdVec); }

    docid64_t SeekDoc(docid64_t docId) override
    {
        if (_cursor >= (int32_t)_docIdVec.size() - 1) {
            return INVALID_DOCID;
        }
        _cursor++;
        return _docIdVec[_cursor];
    }
    index::ErrorCode SeekDocWithErrorCode(docid64_t docId, docid64_t& result) override
    {
        result = SeekDoc(docId);
        return index::ErrorCode::OK;
    }
    void Unpack(TermMatchData& termMatchData) override {}
    bool HasPosition() const override { return false; }
    void Reset() override { _cursor = 0; }
    docpayload_t GetDocPayload() override { return _docPayload[_cursor]; }

private:
    std::vector<docid32_t> _docIdVec;
    int32_t _cursor;
    TermMeta* _termMeta;
    std::vector<docpayload_t> _docPayload;
};

} // namespace indexlib::index
