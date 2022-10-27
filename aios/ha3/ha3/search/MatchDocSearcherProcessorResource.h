#ifndef ISEARCH_MATCHDOCSEARCHERPROCESSORRESOURCE_H
#define ISEARCH_MATCHDOCSEARCHERPROCESSORRESOURCE_H

#include <ha3/search/DocCountLimits.h>
#include <ha3/search/SearchCommonResource.h>
#include <ha3/search/LayerMetas.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <suez/turing/expression/plugin/SorterManager.h>

BEGIN_HA3_NAMESPACE(rank);
class RankProfile;
class ComparatorCreator;
class ScoringProvider;
END_HA3_NAMESPACE(rank);

namespace ha3 {
class ScorerProvider;
}

BEGIN_HA3_NAMESPACE(search);

class SortExpressionCreator;
class SearchProcessor;

struct SearchProcessorResource {
    const rank::RankProfile *rankProfile;
    const suez::turing::SorterManager *sorterManager;
    SortExpressionCreator *sortExpressionCreator;
    rank::ComparatorCreator *comparatorCreator;
    rank::ScoringProvider *scoringProvider;
    suez::turing::AttributeExpression *scoreExpression;
    DocCountLimits &docCountLimits;
    ha3::ScorerProvider *cavaScorerProvider;
    search::LayerMetas *layerMetas;
    SearchProcessorResource(SearchRuntimeResource &runtimeResource)
        : rankProfile(NULL)
        , sorterManager(NULL)
        , sortExpressionCreator(runtimeResource.sortExpressionCreator.get())
        , comparatorCreator(runtimeResource.comparatorCreator.get())
        , scoringProvider(NULL)
        , scoreExpression(NULL)
        , docCountLimits(runtimeResource.docCountLimits)
        , cavaScorerProvider(NULL)
        , layerMetas(NULL)
    {
    }
};

END_HA3_NAMESPACE(search);

#endif //ISEARCH_MATCHDOCSEARCHERPROCESSORRESOURCE_H
