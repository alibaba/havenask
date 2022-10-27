#ifndef ISEARCH_DISTINCTBOOSTSCORER_H
#define ISEARCH_DISTINCTBOOSTSCORER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(rank);

class DistinctBoostScorer
{
public:
    DistinctBoostScorer(){}
    virtual ~DistinctBoostScorer(){}
public:
    virtual score_t calculateBoost(uint32_t keyPos, score_t rawScore) = 0;
};

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_DISTINCTBOOSTSCORER_H
