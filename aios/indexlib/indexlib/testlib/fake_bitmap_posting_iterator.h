#ifndef __INDEXLIB_FAKE_BITMAP_POSTING_ITERATOR_H
#define __INDEXLIB_FAKE_BITMAP_POSTING_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/testlib/fake_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_iterator.h"

IE_NAMESPACE_BEGIN(testlib);

class FakeBitmapPostingIterator : public index::BitmapPostingIterator
{
public:
    FakeBitmapPostingIterator(
            const FakePostingIterator::RichPostings &p, 
            uint32_t statePoolSize,
            autil::mem_pool::Pool *sessionPool = NULL);
    ~FakeBitmapPostingIterator();
private:
    FakeBitmapPostingIterator(const FakeBitmapPostingIterator &);
    FakeBitmapPostingIterator& operator=(const FakeBitmapPostingIterator &);
public:
    IE_NAMESPACE(index)::TermMeta *GetTruncateTermMeta() const override {
        return _truncateTermMeta;
    }
    IE_NAMESPACE(index)::PostingIterator *Clone() const override {
        FakeBitmapPostingIterator* iter = POOL_COMPATIBLE_NEW_CLASS(mSessionPool,
                FakeBitmapPostingIterator, _richPostings, _statePoolSize, mSessionPool);
        return iter;
    }
protected:
    common::ErrorCode InitSingleIterator(SingleIteratorType *singleIter) override;

private:
    FakePostingIterator::RichPostings _richPostings;
    uint32_t _statePoolSize;
    IE_NAMESPACE(index)::TermMeta *_truncateTermMeta;

private:
    static const uint32_t BITMAP_SIZE = 10000;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeBitmapPostingIterator);

IE_NAMESPACE_END(testlib);

#endif //__INDEXLIB_FAKE_BITMAP_POSTING_ITERATOR_H
