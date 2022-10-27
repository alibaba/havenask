#ifndef ISEARCH_AGGREGATORCREATOR_H
#define ISEARCH_AGGREGATORCREATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/common/AggregateClause.h>
#include <ha3/common/AggregateDescription.h>
#include <suez/turing/expression/syntax/SyntaxExpr.h>
#include <ha3/search/Aggregator.h>
#include <ha3/config/AggSamplerConfigInfo.h>
#include <ha3/search/Filter.h>
#include <autil/mem_pool/Pool.h>
#include <suez/turing/expression/framework/AttributeExpressionCreatorBase.h>
#include <suez/turing/common/QueryResource.h>

BEGIN_HA3_NAMESPACE(search);

class AggregatorCreator
{
public:
    AggregatorCreator(suez::turing::AttributeExpressionCreatorBase *attrExprCreator, 
                      autil::mem_pool::Pool *pool,
                      tensorflow::QueryResource *queryResource = NULL,
                      common::Tracer *tracer = NULL);
    ~AggregatorCreator();
public:
    Aggregator* createAggregator(const common::AggregateClause* aggClause);
    common::ErrorResult getErrorResult() { return _errorResult; }
    void setAggSamplerConfigInfo(const config::AggSamplerConfigInfo &aggSamplerConfigInfo) {
        _aggSamplerConfigInfo = aggSamplerConfigInfo;
    }
private:
    AggregateFunction *createAggregateFunction(
            const common::AggFunDescription *aggFunDes);
    template<typename ResultType, typename T>
    AggregateFunction *doCreateAggregateFunction(
            const common::AggFunDescription *aggFunDes);
    AggregateFunction *
    doCreateDistinctCountAggFunction(const common::AggFunDescription *aggFunDes);

    Aggregator *
    createSingleAggregator(const common::AggregateDescription *aggDes);
    template <typename KeyType>
    Aggregator *doCreateSingleAggregatorDelegator(
        const common::AggregateDescription *aggDes);
    template <typename KeyType, typename ExprType, typename GroupMapType>
    Aggregator *
    doCreateSingleAggregator(const common::AggregateDescription *aggDes);

    Filter *createAggregateFilter(const common::AggregateDescription *aggDes);

  private:
    suez::turing::AttributeExpressionCreatorBase *_attrExprCreator;
    autil::mem_pool::Pool *_pool;
    common::ErrorResult _errorResult;
    config::AggSamplerConfigInfo _aggSamplerConfigInfo;
  private:
    suez::turing::SuezCavaAllocator *_cavaAllocator;
    tensorflow::QueryResource *_queryResource;
    suez::turing::Tracer *_tracer;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(AggregatorCreator);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_AGGREGATORCREATOR_H
