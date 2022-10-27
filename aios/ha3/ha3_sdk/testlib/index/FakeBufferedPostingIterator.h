#ifndef ISEARCH_FAKEBUFFEREDPOSTINGITERATOR_H
#define ISEARCH_FAKEBUFFEREDPOSTINGITERATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <indexlib/index/normal/inverted_index/accessor/buffered_posting_iterator.h>
#include <ha3_sdk/testlib/index/FakeBufferedIndexDecoder.h>

IE_NAMESPACE_BEGIN(index);

class FakeBufferedPostingIterator : public BufferedPostingIterator
{
public:
    FakeBufferedPostingIterator(
            const index::PostingFormatOption& postingFormatOption,
            autil::mem_pool::Pool *sessionPool);
    ~FakeBufferedPostingIterator() {
        if(_termMeta) {
            delete _termMeta;
        }
    }
private:
    FakeBufferedPostingIterator(const FakeBufferedPostingIterator &);
    FakeBufferedPostingIterator& operator=(const FakeBufferedPostingIterator &);
public:
    void init(const std::string &docIdStr,
              const std::string &fieldMapStr,
              uint32_t statePoolSize)
    {
        FakeBufferedIndexDecoder *decoder = POOL_COMPATIBLE_NEW_CLASS(mSessionPool,
                FakeBufferedIndexDecoder, mPostingFormatOption, mSessionPool);
        decoder->Init(docIdStr, fieldMapStr);
        mDecoder = decoder;
        mStatePool.Init(statePoolSize);
        _termMeta = new index::TermMeta();
    }
    /* override */ index::TermMeta* GetTermMeta() const { return _termMeta; }
private:
    index::TermMeta *_termMeta;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakeBufferedPostingIterator);

IE_NAMESPACE_END(index);

#endif //ISEARCH_FAKEBUFFEREDPOSTINGITERATOR_H
