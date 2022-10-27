#ifndef ISEARCH_RANGETERMQUERYEXECUTOR_H
#define ISEARCH_RANGETERMQUERYEXECUTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/TermQueryExecutor.h>
#include <indexlib/index/normal/inverted_index/accessor/range_posting_iterator.h>

BEGIN_HA3_NAMESPACE(search);

class RangeTermQueryExecutor : public TermQueryExecutor
{
public:
    RangeTermQueryExecutor(IE_NAMESPACE(index)::PostingIterator *iter, 
                          const common::Term &term);
    ~RangeTermQueryExecutor();
private:
    RangeTermQueryExecutor(const RangeTermQueryExecutor &);
    RangeTermQueryExecutor& operator=(const RangeTermQueryExecutor &);
public:
    const std::string getName() const override { return "RangeTermQueryExecutor";}
    void reset() override {
        TermQueryExecutor::reset();
        initRangePostingIterator();
    }
    uint32_t getSeekDocCount() override { return _rangeIter->GetSeekDocCount(); }
    /* override */ IE_NAMESPACE(index)::DocValueFilter* stealFilter() override
    {
        IE_NAMESPACE(index)::DocValueFilter *tmp = _filter;
        _filter = NULL;
        return tmp;
    }
private:
    void initRangePostingIterator();
protected:
    IE_NAMESPACE(common)::ErrorCode doSeek(docid_t id, docid_t& result) override;
protected:
    IE_NAMESPACE(index)::RangePostingIterator *_rangeIter;
    IE_NAMESPACE(index)::DocValueFilter *_filter;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(RangeTermQueryExecutor);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_RANGETERMQUERYEXECUTOR_H
