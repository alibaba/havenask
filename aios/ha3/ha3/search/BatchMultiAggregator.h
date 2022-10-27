#ifndef ISEARCH_BATCHMULTIAGGREGATOR_H
#define ISEARCH_BATCHMULTIAGGREGATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/MultiAggregator.h>
#include <ha3/search/BatchAggregator.h>

BEGIN_HA3_NAMESPACE(search);

class BatchMultiAggregator : public MultiAggregator
{
public:
    BatchMultiAggregator(autil::mem_pool::Pool *pool);
    BatchMultiAggregator(const AggregatorVector &aggVec, autil::mem_pool::Pool *pool);
    virtual ~BatchMultiAggregator();
private:
    BatchMultiAggregator(const BatchMultiAggregator &);
    BatchMultiAggregator& operator=(const BatchMultiAggregator &);
public:
    void doAggregate(matchdoc::MatchDoc matchDoc) override;
    void estimateResult(double factor) override;
    void endLayer(double factor) override;
    std::string getName() override {
        return "batchMultiAgg";
    }
    uint32_t getAggregateCount() override {
        return _sampler.getAggregateCount();
    }
private:
    BatchAggregateSampler _sampler;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(BatchMultiAggregator);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_BATCHMULTIAGGREGATOR_H
