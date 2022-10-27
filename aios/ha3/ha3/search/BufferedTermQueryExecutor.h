#ifndef ISEARCH_BUFFEREDTERMQUERYEXECUTOR_H
#define ISEARCH_BUFFEREDTERMQUERYEXECUTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/TermQueryExecutor.h>
#include <indexlib/index/normal/inverted_index/accessor/buffered_posting_iterator.h>

BEGIN_HA3_NAMESPACE(search);

class BufferedTermQueryExecutor : public TermQueryExecutor
{
public:
    BufferedTermQueryExecutor(IE_NAMESPACE(index)::PostingIterator *iter, 
                              const common::Term &term);
    ~BufferedTermQueryExecutor();
private:
    BufferedTermQueryExecutor(const BufferedTermQueryExecutor &);
    BufferedTermQueryExecutor& operator=(const BufferedTermQueryExecutor &);
public:
    const std::string getName() const override {
        return "BufferedTermQueryExecutor";
    }
    void reset() override {
        TermQueryExecutor::reset();
        initBufferedPostingIterator();
    }
private:
    void initBufferedPostingIterator();
protected:
    IE_NAMESPACE(common)::ErrorCode doSeek(docid_t id, docid_t& result) override;
protected:
    IE_NAMESPACE(index)::BufferedPostingIterator *_bufferedIter;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(BufferedTermQueryExecutor);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_BUFFEREDTERMQUERYEXECUTOR_H
