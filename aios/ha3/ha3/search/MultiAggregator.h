#ifndef ISEARCH_MULTIAGGREGATOR_H
#define ISEARCH_MULTIAGGREGATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <vector>
#include <ha3/search/Aggregator.h>
#include <ha3/search/NormalAggregator.h>

BEGIN_HA3_NAMESPACE(search);

class MultiAggregator : public Aggregator
{
public:
    typedef std::vector<Aggregator*> AggregatorVector;
public:
    MultiAggregator(autil::mem_pool::Pool *pool);
    MultiAggregator(const AggregatorVector &aggVec,
                    autil::mem_pool::Pool *pool);
    ~MultiAggregator();
public:
    void batchAggregate(std::vector<matchdoc::MatchDoc> &matchDocs) override {
        _toAggDocCount += matchDocs.size();
        for (AggregatorVector::iterator it = _aggVector.begin();
             it != _aggVector.end(); it++)
        {
            (*it)->batchAggregate(matchDocs);
        }
    }
    void doAggregate(matchdoc::MatchDoc matchDoc) override;
    void addAggregator(Aggregator *agg) {
        _aggVector.push_back(agg);
    }
    void setMatchDocAllocator(matchdoc::MatchDocAllocator *alloc) override {
        for (size_t i = 0; i < _aggVector.size(); ++i) {
            _aggVector[i]->setMatchDocAllocator(alloc);
        }
        this->_matchDocAllocator = alloc;
    }
    common::AggregateResultsPtr collectAggregateResult() override;
    void estimateResult(double factor) override;
    void beginSample() override;
    void endLayer(double factor) override;
    uint32_t getAggregateCount() override {
        assert (!_aggVector.empty());
        return _aggVector[0]->getAggregateCount();
    }
    void updateExprEvaluatedStatus() override;

    template <typename KeyType, typename ExprType, typename GroupMapType>
    const NormalAggregator<KeyType, ExprType, GroupMapType> *getNormalAggregator(uint32_t pos);

protected:
    AggregatorVector _aggVector;
private:
    HA3_LOG_DECLARE();
};

template <typename KeyType, typename ExprType, typename GroupMapType>
const NormalAggregator<
    KeyType, 
    ExprType, 
    GroupMapType> *MultiAggregator::getNormalAggregator(uint32_t pos)
{
    if (pos >= _aggVector.size()) {
        return NULL;
    }
    return dynamic_cast<
        NormalAggregator<KeyType, ExprType, GroupMapType>*>(_aggVector[pos]);
}

END_HA3_NAMESPACE(search);

#endif //ISEARCH_MULTIAGGREGATOR_H
