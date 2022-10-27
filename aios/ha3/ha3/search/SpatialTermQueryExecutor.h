#ifndef ISEARCH_SPATIALTERMQUERYEXECUTOR_H
#define ISEARCH_SPATIALTERMQUERYEXECUTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/TermQueryExecutor.h>
#include <indexlib/index/normal/inverted_index/builtin_index/spatial/spatial_posting_iterator.h>

BEGIN_HA3_NAMESPACE(search);

class SpatialTermQueryExecutor : public TermQueryExecutor
{
public:
    SpatialTermQueryExecutor(IE_NAMESPACE(index)::PostingIterator *iter,
                             const common::Term &term);
    ~SpatialTermQueryExecutor();
private:
    SpatialTermQueryExecutor(const SpatialTermQueryExecutor &);
    SpatialTermQueryExecutor& operator=(const SpatialTermQueryExecutor &);
public:
     const std::string getName() const override { return "SpatialTermQueryExecutor";}
     IE_NAMESPACE(common)::ErrorCode doSeek(docid_t id, docid_t& result) override;
     void reset() override {
        TermQueryExecutor::reset();
        initSpatialPostingIterator();
    }
    uint32_t getSeekDocCount() override { return _spatialIter->GetSeekDocCount() + _testDocCount; }
    /* override */ IE_NAMESPACE(index)::DocValueFilter* stealFilter() override
    {
        IE_NAMESPACE(index)::DocValueFilter *tmp = _filter;
        _filter = NULL;
        return tmp;
    }
private:
    void initSpatialPostingIterator();
private:
    IE_NAMESPACE(index)::SpatialPostingIterator *_spatialIter;
    IE_NAMESPACE(index)::DocValueFilter *_filter;
    int64_t _testDocCount;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SpatialTermQueryExecutor);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_SPATIALTERMQUERYEXECUTOR_H
