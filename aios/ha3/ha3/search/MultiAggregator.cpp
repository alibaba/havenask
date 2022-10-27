#include <ha3/search/MultiAggregator.h>
#include <sstream>

using namespace std;
using namespace autil;
BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, MultiAggregator);
USE_HA3_NAMESPACE(common);

MultiAggregator::MultiAggregator(mem_pool::Pool *pool) 
  : Aggregator(pool) 
{
}

MultiAggregator::MultiAggregator(const AggregatorVector& aggVec, 
                                 mem_pool::Pool *pool)
    : Aggregator(pool)
    , _aggVector(aggVec)
{
}

MultiAggregator::~MultiAggregator() { 
    for (AggregatorVector::iterator it = _aggVector.begin();
         it != _aggVector.end(); it++)
    {
        delete (*it);
    }
    _aggVector.clear();
}

void MultiAggregator::doAggregate(matchdoc::MatchDoc matchDoc) {
    for (AggregatorVector::iterator it = _aggVector.begin();
         it != _aggVector.end(); it++)
    {
        (*it)->doAggregate(matchDoc);
    }
}

common::AggregateResultsPtr MultiAggregator::collectAggregateResult() {
    common::AggregateResultsPtr aggResultsPtr(new common::AggregateResults());
    for (AggregatorVector::const_iterator it = _aggVector.begin();
         it != _aggVector.end(); it++)
    {
        const common::AggregateResultsPtr tmpAggResultsPtr =
            (*it)->collectAggregateResult();
        aggResultsPtr->insert(aggResultsPtr->end(),
                              tmpAggResultsPtr->begin(),
                              tmpAggResultsPtr->end());
    }
    return aggResultsPtr;
}

void MultiAggregator::estimateResult(double factor) {
    for (AggregatorVector::const_iterator it = _aggVector.begin();
         it != _aggVector.end(); it++)
    {
        (*it)->estimateResult(factor);
    }
}

void MultiAggregator::beginSample() {
    for (AggregatorVector::const_iterator it = _aggVector.begin();
         it != _aggVector.end(); it++)
    {
        (*it)->beginSample();
    }
}

void MultiAggregator::endLayer(double factor) {
    for (AggregatorVector::const_iterator it = _aggVector.begin();
         it != _aggVector.end(); it++)
    {
        (*it)->endLayer(factor);
    }
}

void MultiAggregator::updateExprEvaluatedStatus() {
    for (AggregatorVector::const_iterator it = _aggVector.begin();
         it != _aggVector.end(); it++)
    {
        (*it)->updateExprEvaluatedStatus();
    }
}

END_HA3_NAMESPACE(search);
