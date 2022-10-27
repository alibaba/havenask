#ifndef ISEARCH_SCORER_H
#define ISEARCH_SCORER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/plugin/Scorer.h>
#include <suez/turing/expression/provider/ScoringProvider.h>
#include <matchdoc/MatchDoc.h>

BEGIN_HA3_NAMESPACE(rank);

typedef suez::turing::ScorerParam ScorerParam; 
typedef suez::turing::Scorer Scorer; 

END_HA3_NAMESPACE(rank);
 
#endif //ISEARCH_SCORER_H
