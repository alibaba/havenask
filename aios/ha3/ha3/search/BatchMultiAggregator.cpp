#include <ha3/search/BatchMultiAggregator.h>

BEGIN_HA3_NAMESPACE(search);

using namespace autil;

HA3_LOG_SETUP(search, BatchMultiAggregator);

BatchMultiAggregator::BatchMultiAggregator(mem_pool::Pool *pool)
  : MultiAggregator(pool) 
{
}

BatchMultiAggregator::BatchMultiAggregator(
        const AggregatorVector &aggVec, mem_pool::Pool *pool) 
    : MultiAggregator(aggVec, pool)
{ 
}

BatchMultiAggregator::~BatchMultiAggregator() { 
}

void BatchMultiAggregator::doAggregate(matchdoc::MatchDoc matchDoc) {
    _sampler.collect(matchDoc, _matchDocAllocator);
}

void BatchMultiAggregator::estimateResult(double factor) {
    for (AggregatorVector::const_iterator it = _aggVector.begin();
         it != _aggVector.end(); it++)
    {
        _sampler.reStart();
        (*it)->estimateResult(factor, _sampler);
    }
}

void BatchMultiAggregator::endLayer(double factor) {
    _sampler.endLayer(factor);
}

END_HA3_NAMESPACE(search);

