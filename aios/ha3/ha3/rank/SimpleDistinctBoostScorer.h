#ifndef ISEARCH_SIMPLEDISTINCTBOOSTSCORER_H
#define ISEARCH_SIMPLEDISTINCTBOOSTSCORER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/DistinctBoostScorer.h>

BEGIN_HA3_NAMESPACE(rank);

class SimpleDistinctBoostScorer : public DistinctBoostScorer
{
public:
    SimpleDistinctBoostScorer(uint32_t distinctTimes, uint32_t distinctCount);
    ~SimpleDistinctBoostScorer();
public:
    score_t calculateBoost(uint32_t keyPosition, score_t rawScore);
private:
    uint32_t _distinctTimes;
    uint32_t _distinctCount;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_SIMPLEDISTINCTBOOSTSCORER_H
