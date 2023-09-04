#pragma once

#include <memory>
#include <stddef.h>
#include <stdint.h>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3_sdk/testlib/index/FakeTextIndexReader.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/inverted_index/PositionIteratorTyped.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingIterator.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/misc/common.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace indexlib {
namespace index {

class FakeBitmapPostingIterator : public indexlib::index::BitmapPostingIterator {
public:
    FakeBitmapPostingIterator(const FakeTextIndexReader::RichPostings &p,
                              uint32_t statePoolSize,
                              autil::mem_pool::Pool *sessionPool = NULL);
    ~FakeBitmapPostingIterator();

private:
    FakeBitmapPostingIterator(const FakeBitmapPostingIterator &);
    FakeBitmapPostingIterator &operator=(const FakeBitmapPostingIterator &);

public:
    /*override*/ indexlib::index::TermMeta *GetTruncateTermMeta() const override {
        return _truncateTermMeta;
    }
    /* override */ indexlib::index::PostingIterator *Clone() const override {
        return new FakeBitmapPostingIterator(_richPostings, _statePoolSize, _sessionPool);
    }

protected:
    indexlib::index::ErrorCode InitSingleIterator(SingleIteratorType *singleIter) override;

private:
    FakeTextIndexReader::RichPostings _richPostings;
    uint32_t _statePoolSize;
    indexlib::index::TermMeta *_truncateTermMeta;

private:
    static const uint32_t BITMAP_SIZE = 10000;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FakeBitmapPostingIterator> FakeBitmapPostingIteratorPtr;

} // namespace index
} // namespace indexlib
