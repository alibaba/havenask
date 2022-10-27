#ifndef ISEARCH_FAKEBITMAPPOSTINGITERATOR_H
#define ISEARCH_FAKEBITMAPPOSTINGITERATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3_sdk/testlib/index/FakeTextIndexReader.h>
#include <indexlib/common_define.h>
#include <indexlib/index/normal/inverted_index/accessor/posting_iterator.h>
#include <indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_iterator.h>
IE_NAMESPACE_BEGIN(index);

class FakeBitmapPostingIterator : public BitmapPostingIterator
{
public:
    FakeBitmapPostingIterator(
            const FakeTextIndexReader::RichPostings &p, 
            uint32_t statePoolSize,
            autil::mem_pool::Pool *sessionPool = NULL);
    ~FakeBitmapPostingIterator();
private:
    FakeBitmapPostingIterator(const FakeBitmapPostingIterator &);
    FakeBitmapPostingIterator& operator=(const FakeBitmapPostingIterator &);
public:
    /*override*/ IE_NAMESPACE(index)::TermMeta *GetTruncateTermMeta() const override {
        return _truncateTermMeta;
    }
    /* override */ IE_NAMESPACE(index)::PostingIterator *Clone() const override {
        return new FakeBitmapPostingIterator(_richPostings, _statePoolSize, mSessionPool);
    }
protected:
    IE_NAMESPACE(common)::ErrorCode InitSingleIterator(SingleIteratorType *singleIter) override;

private:
    FakeTextIndexReader::RichPostings _richPostings;
    uint32_t _statePoolSize;
    IE_NAMESPACE(index)::TermMeta *_truncateTermMeta;
private:
    static const uint32_t BITMAP_SIZE = 10000;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakeBitmapPostingIterator);

IE_NAMESPACE_END(index);

#endif //ISEARCH_FAKEBITMAPPOSTINGITERATOR_H
