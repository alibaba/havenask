#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingIterator.h"
#include "indexlib/indexlib.h"
#include "indexlib/testlib/fake_posting_iterator.h"

namespace indexlib { namespace testlib {

class FakeBitmapPostingIterator : public index::BitmapPostingIterator
{
public:
    FakeBitmapPostingIterator(const FakePostingIterator::RichPostings& p, uint32_t statePoolSize,
                              autil::mem_pool::Pool* sessionPool = NULL);
    ~FakeBitmapPostingIterator();

private:
    FakeBitmapPostingIterator(const FakeBitmapPostingIterator&);
    FakeBitmapPostingIterator& operator=(const FakeBitmapPostingIterator&);

public:
    index::TermMeta* GetTruncateTermMeta() const override { return _truncateTermMeta; }
    index::PostingIterator* Clone() const override
    {
        FakeBitmapPostingIterator* iter = POOL_COMPATIBLE_NEW_CLASS(_sessionPool, FakeBitmapPostingIterator,
                                                                    _richPostings, _statePoolSize, _sessionPool);
        return iter;
    }

protected:
    index::ErrorCode InitSingleIterator(SingleIteratorType* singleIter) override;

private:
    FakePostingIterator::RichPostings _richPostings;
    uint32_t _statePoolSize;
    index::TermMeta* _truncateTermMeta;

private:
    static const uint32_t BITMAP_SIZE = 10000;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeBitmapPostingIterator);
}} // namespace indexlib::testlib
