#ifndef ISEARCH_SCORERMODULEFACTORY_H
#define ISEARCH_SCORERMODULEFACTORY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/ModuleFactory.h>
#include <ha3/rank/Scorer.h>
#include <ha3/config/ResourceReader.h>
#include <suez/turing/expression/plugin/ScorerModuleFactory.h>

BEGIN_HA3_NAMESPACE(rank);

typedef suez::turing::ScorerModuleFactory ScorerModuleFactory; 

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_SCORERMODULEFACTORY_H
