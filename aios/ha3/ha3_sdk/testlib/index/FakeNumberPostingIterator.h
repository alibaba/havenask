#pragma once
#include <assert.h>
#include <stddef.h>
#include <stdint.h>

#include "autil/CommonMacros.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "autil/mem_pool/Pool.h"
#include "ha3_sdk/testlib/index/FakeNumberIndexReader.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace indexlib {
namespace index {
class TermMatchData;
} // namespace index
} // namespace indexlib

namespace indexlib {
namespace index {

class FakeNumberPostingIterator : public PostingIterator {
public:
    FakeNumberPostingIterator(FakeNumberIndexReader::NumberValuesPtr numberValuePtr,
                              int32_t leftNumber,
                              int32_t rightNumber,
                              autil::mem_pool::Pool *sessionPool = NULL)
        : _numberValuePtr(numberValuePtr)
        , _leftNumber(leftNumber)
        , _rightNumber(rightNumber)
        , _sessionPool(sessionPool)
        , _termMeta(NULL) {
        _termMeta = new indexlib::index::TermMeta();
    }

    virtual ~FakeNumberPostingIterator() {
        DELETE_AND_SET_NULL(_termMeta);
    }

    virtual indexlib::index::TermMeta *GetTermMeta() const override;

    indexlib::docid64_t SeekDoc(docid64_t id) override;
    indexlib::index::ErrorCode SeekDocWithErrorCode(docid64_t docId,
                                                    indexlib::docid64_t &result) override {
        result = SeekDoc(docId);
        return indexlib::index::ErrorCode::OK;
    }

    virtual void Unpack(TermMatchData &termMatchData) override;
    bool HasPosition() const override {
        return true;
    }
    autil::mem_pool::Pool *GetSessionPool() const override {
        return _sessionPool;
    }
    void Reset() override {}
    virtual PostingIterator *Clone() const override {
        assert(false);
        return NULL;
    }

private:
    bool valueInRange(int32_t value);

private:
    FakeNumberIndexReader::NumberValuesPtr _numberValuePtr;
    int32_t _leftNumber;
    int32_t _rightNumber;
    autil::mem_pool::Pool *_sessionPool;
    indexlib::index::TermMeta *_termMeta;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace index
} // namespace indexlib
