#include <ha3/rank/SimpleDistinctBoostScorer.h>

BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, SimpleDistinctBoostScorer);

SimpleDistinctBoostScorer::SimpleDistinctBoostScorer(uint32_t distinctTimes, 
        uint32_t distinctCount)
{ 
    _distinctTimes = distinctTimes;
    _distinctCount = distinctCount;
}

SimpleDistinctBoostScorer::~SimpleDistinctBoostScorer() { 
}

score_t SimpleDistinctBoostScorer::calculateBoost(uint32_t keyPosition, 
        score_t rawScore)
{
    uint32_t time = keyPosition / _distinctCount;
    if (time >= _distinctTimes) {
        return (score_t)0;
    } else {
        return (score_t)(_distinctTimes - time);
    }
}

END_HA3_NAMESPACE(rank);
