#ifndef ISEARCH_ALLDOCTERMQUERYEXECUTOR_H
#define ISEARCH_ALLDOCTERMQUERYEXECUTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/TermQueryExecutor.h>
#include <indexlib/index/normal/inverted_index/accessor/range_posting_iterator.h>

BEGIN_HA3_NAMESPACE(search);

class AllDocTermQueryExecutor : public TermQueryExecutor
{
public:
    AllDocTermQueryExecutor(IE_NAMESPACE(index)::PostingIterator *iter, 
                          const common::Term &term);
    ~AllDocTermQueryExecutor();
private:
    AllDocTermQueryExecutor(const AllDocTermQueryExecutor &);
    AllDocTermQueryExecutor& operator=(const AllDocTermQueryExecutor &);
public:
    const std::string getName() const override { return "AllDocTermQueryExecutor";}
    /* void reset() override { */
    /*     TermQueryExecutor::reset(); */
    /*     initRangePostingIterator(); */
    /* } */
    /* uint32_t getSeekDocCount() override { return _rangeIter->GetSeekDocCount(); } */
    /* /\* override *\/ IE_NAMESPACE(index)::DocValueFilter* stealFilter() */
    /* { */
    /*     IE_NAMESPACE(index)::DocValueFilter *tmp = _filter; */
    /*     _filter = NULL; */
    /*     return tmp; */
    /* } */
/* private: */
/*     void initRangePostingIterator(); */
/* protected: */
/*     docid_t doSeek(docid_t id) override; */
/* protected: */
/*     IE_NAMESPACE(index)::RangePostingIterator *_rangeIter; */
/*     IE_NAMESPACE(index)::DocValueFilter *_filter; */
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(AllDocTermQueryExecutor);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_ALLDOCTERMQUERYEXECUTOR_H
