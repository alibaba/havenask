#include <ha3/search/AggregatorCreator.h>
#include <ha3/search/Aggregator.h>
#include <ha3/search/MultiAggregator.h>
#include <ha3/search/RangeAggregator.h>
#include <ha3/search/BatchAggregator.h>
#include <ha3/search/BatchMultiAggregator.h>
#include <ha3/search/JitNormalAggregator.h>
#include <ha3/search/TupleAggregator.h>
#include <ha3/search/CountAggregateFunction.h>
#include <ha3/search/MaxAggregateFunction.h>
#include <ha3/search/MinAggregateFunction.h>
#include <ha3/search/SumAggregateFunction.h>
#include <ha3/search/DistinctCountAggregateFunction.h>
#include <ha3/queryparser/RequestSymbolDefine.h>
#include <algorithm>
#include <iterator>
#include <iostream>
#include <autil/StringUtil.h>
#include <suez/turing/expression/framework/VariableTypeTraits.h>


using namespace std;
using namespace suez::turing;

namespace autil{
template<>
inline MultiChar
StringUtil::fromString<MultiChar>(const std::string& str) {
    MultiChar ret;
    assert(false);
    return ret;
}
}

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, AggregatorCreator);

AggregatorCreator::AggregatorCreator(AttributeExpressionCreatorBase *attrExprCreator,
                                     autil::mem_pool::Pool *pool,
                                     tensorflow::QueryResource *queryResource,
                                     common::Tracer *tracer)
    : _attrExprCreator(attrExprCreator)
    , _pool(pool)
    , _cavaAllocator(queryResource ? queryResource->getCavaAllocator() : NULL)
    , _queryResource(queryResource)
    , _tracer(queryResource ? queryResource->getTracer() : tracer)
{
}

AggregatorCreator::~AggregatorCreator() {
}

Aggregator* AggregatorCreator::createAggregator(const AggregateClause* aggClause) {
    if (!aggClause) {
        HA3_LOG(TRACE3, "AggregateClause is NULL");
        return NULL;
    }

    const AggregateDescriptions& aggDescriptions
        = aggClause->getAggDescriptions();
    vector<Aggregator*> aggVec;
    for (AggregateDescriptions::const_iterator it = aggDescriptions.begin();
         it != aggDescriptions.end(); it++)
    {
        const AggregateDescription *aggDes = *it;
        Aggregator *aggregator = createSingleAggregator(aggDes);
        if (aggregator) {
            aggVec.push_back(aggregator);
        } else {
            HA3_LOG(WARN, "Create aggregator error from aggDes[%s]",
                aggDes->getOriginalString().c_str());
            for (size_t i = 0; i < aggVec.size(); ++i) {
                delete aggVec[i];
            }
            aggVec.clear();
            return NULL;
        }
    }

    if (aggVec.empty()) {
        HA3_LOG(WARN, "Create aggregator error from aggClause[%s]",
                aggClause->getOriginalString().c_str());
    } else if (aggVec.size() == 1) {
        return aggVec[0];
    }

    Aggregator* retAggregator = NULL;
    if (_aggSamplerConfigInfo.getAggBatchMode()) {
        retAggregator = new BatchMultiAggregator(aggVec, _pool);
    } else {
        retAggregator = new MultiAggregator(aggVec, _pool);
    }
    return retAggregator;
}

template<typename ResultType, typename T>
AggregateFunction* AggregatorCreator::doCreateAggregateFunction(const AggFunDescription* aggFunDes) {
    const SyntaxExpr *syntaxExpr = aggFunDes->getSyntaxExpr();
    AttributeExpression *attrExpr =
        _attrExprCreator->createAttributeExpression(syntaxExpr);
    if (!attrExpr) {
        HA3_LOG(WARN, "create aggregate function expression[%s] failed.",
                syntaxExpr->getExprString().c_str());
        return NULL;
    }
    AttributeExpressionTyped<T> *expr =
        dynamic_cast<AttributeExpressionTyped<T> *>(attrExpr);
     if (!expr) {
         HA3_LOG(WARN, "unexpected type for aggregator function[%s].",
                 syntaxExpr->getExprString().c_str());
         return NULL;
    }

    AggregateFunction* aggFun = NULL;
    const string& functionName = aggFunDes->getFunctionName();
    if (functionName == AGGREGATE_FUNCTION_COUNT) {
        // count function is handled in createAggregateFunction
        assert(false);
    } else if (functionName == AGGREGATE_FUNCTION_MAX) {
        aggFun = POOL_NEW_CLASS(_pool, MaxAggregateFunction<T>,
                                expr->getOriginalString(),
                                syntaxExpr->getExprResultType(), expr);
    } else if (functionName == AGGREGATE_FUNCTION_MIN) {
        aggFun = POOL_NEW_CLASS(_pool, MinAggregateFunction<T>,
                                expr->getOriginalString(),
                                syntaxExpr->getExprResultType(), expr);
    } else if (functionName == AGGREGATE_FUNCTION_SUM) {
        aggFun =
            new (_pool->allocate(sizeof(SumAggregateFunction<ResultType, T>)))
                SumAggregateFunction<ResultType, T>(
                    expr->getOriginalString(), syntaxExpr->getExprResultType(),
                    expr);
    } else if (functionName == AGGREGATE_FUNCTION_DISTINCT_COUNT) {
        assert(false);
    } else {
        string errMsg = string("not support aggregate function: [") + functionName + "]";
        HA3_LOG(WARN, "%s", errMsg.c_str());
        _errorResult.setErrorCode(ERROR_AGG_FUN_NOT_SUPPORT);
        _errorResult.setErrorMsg(functionName);
        aggFun = NULL;
    }

    return aggFun;
}

AggregateFunction* AggregatorCreator::doCreateDistinctCountAggFunction(const AggFunDescription* aggFunDes) {
    const SyntaxExpr *syntaxExpr = aggFunDes->getSyntaxExpr();
    AttributeExpression *attrExpr =
        _attrExprCreator->createAttributeExpression(syntaxExpr);
    if (!attrExpr) {
        HA3_LOG(WARN, "create distinct_count aggregate function expression[%s] failed.",
                syntaxExpr->getExprString().c_str());
        return NULL;
    }

    AggregateFunction* aggFun = NULL;

    VariableType vt = attrExpr->getType();
    bool isMulti = attrExpr->isMultiValue();
#define CREATE_DISTINCT_COUNT_AGG_FUNCTION_SINGLE(vt_type)                     \
    case vt_type: {                                                            \
        typedef VariableTypeTraits<vt_type, false>::AttrExprType ExprType;      \
        AttributeExpressionTyped<ExprType> *expr =                             \
            dynamic_cast<AttributeExpressionTyped<ExprType> *>(attrExpr);      \
        if (expr == NULL) {                                                    \
            HA3_LOG(WARN, "unexpected type for aggregator function[%s].",      \
                    syntaxExpr->getExprString().c_str());                      \
            return NULL;                                                       \
        }                                                                      \
        aggFun =                                                               \
            POOL_NEW_CLASS(_pool, DistinctCountAggregateFunction<ExprType>,    \
                           expr->getOriginalString(), expr);                   \
    } break

#define CREATE_DISTINCT_COUNT_AGG_FUNCTION_MULTI(vt_type)                      \
    case vt_type: {                                                            \
        typedef VariableTypeTraits<vt_type, true>::AttrExprType ExprType;      \
        typedef VariableTypeTraits<vt_type, false>::AttrExprType               \
            SingleExprType;                                                    \
        typedef DistinctCountAggregateFunction<ExprType, SingleExprType>       \
            DistinctCountAggregateFunctionType;                                \
        AttributeExpressionTyped<ExprType> *expr =                             \
            dynamic_cast<AttributeExpressionTyped<ExprType> *>(attrExpr);      \
        if (expr == NULL) {                                                    \
            HA3_LOG(WARN, "unexpected type for aggregator function[%s].",      \
                    syntaxExpr->getExprString().c_str());                      \
            return NULL;                                                       \
        }                                                                      \
        aggFun = POOL_NEW_CLASS(_pool, DistinctCountAggregateFunctionType,     \
                                expr->getOriginalString(), expr);              \
    } break
    if (isMulti) {
        switch (vt) {
            NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL_AND_STRING(
                CREATE_DISTINCT_COUNT_AGG_FUNCTION_MULTI);
        default:
            break;
        }
    } else {
        switch (vt) {
            NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL_AND_STRING(
                CREATE_DISTINCT_COUNT_AGG_FUNCTION_SINGLE);
        default:
            break;
        }
    }
    #undef CREATE_DISTINCT_COUNT_AGG_FUNCTION_MULTI
    #undef CREATE_DISTINCT_COUNT_AGG_FUNCTION_SINGLE

    return aggFun;
}

AggregateFunction *AggregatorCreator::createAggregateFunction(
        const AggFunDescription *aggFunDes)
{
    AggregateFunction *aggFun = NULL;
    if (aggFunDes->getFunctionName() == AGGREGATE_FUNCTION_COUNT) {
        //'CountAggregateFunction' has no AttributeExpression
        return POOL_NEW_CLASS(_pool, CountAggregateFunction);
    } else if (aggFunDes->getFunctionName() == AGGREGATE_FUNCTION_DISTINCT_COUNT) {
        aggFun = doCreateDistinctCountAggFunction(aggFunDes);
        return aggFun;
    }
    if (aggFunDes->getSyntaxExpr() == NULL) {
        return NULL;
    }
    auto variableType = aggFunDes->getSyntaxExpr()->getExprResultType();

#define CREATE_AGG_FUN_HELPER(vt_type, ResultType)                      \
    case vt_type:                                                       \
        {                                                               \
            typedef VariableTypeTraits<vt_type, false>::AttrExprType T; \
            aggFun = doCreateAggregateFunction<ResultType, T>(aggFunDes); \
        }                                                               \
    break

    switch(variableType) {
        CREATE_AGG_FUN_HELPER(vt_bool, int64_t);
        CREATE_AGG_FUN_HELPER(vt_int8, int64_t);
        CREATE_AGG_FUN_HELPER(vt_int16, int64_t);
        CREATE_AGG_FUN_HELPER(vt_int32, int64_t);
        CREATE_AGG_FUN_HELPER(vt_int64, int64_t);
        CREATE_AGG_FUN_HELPER(vt_uint8, int64_t);
        CREATE_AGG_FUN_HELPER(vt_uint16, int64_t);
        CREATE_AGG_FUN_HELPER(vt_uint32, int64_t);
        CREATE_AGG_FUN_HELPER(vt_uint64, uint64_t);
        CREATE_AGG_FUN_HELPER(vt_float, float);
        CREATE_AGG_FUN_HELPER(vt_double, double);
    default:
        aggFun = NULL;
        // for unsupported value type
        break;
    }
    if (aggFun == NULL) {
        HA3_LOG(WARN, "does not support this Aggregator Function Result type: %d",
            variableType);
        _errorResult.setErrorCode(ERROR_AGG_FUN_TYPE_NOT_SUPPORT);
        stringstream ss;
        ss << "Result Type: " << vt2TypeName(variableType);
        _errorResult.setErrorMsg(ss.str());
    }
#undef CREATE_AGG_FUN_HELPER
    return aggFun;
}

Filter* AggregatorCreator::createAggregateFilter(const AggregateDescription *aggDes) {
    FilterClause* filterClause = aggDes->getFilterClause();
    return Filter::createFilter(filterClause, _attrExprCreator, _pool);
}

template<typename KeyType>
Aggregator * AggregatorCreator::doCreateSingleAggregatorDelegator(const AggregateDescription *aggDes) {
    SyntaxExpr *groupKeyExpr = aggDes->getGroupKeyExpr();

    bool enableDenseMode = _aggSamplerConfigInfo.isEnableDenseMode();
    if (enableDenseMode) {
        typedef typename DenseMapTraits<KeyType>::GroupMapType GroupDenseMapType;
        if (groupKeyExpr->isMultiValue()) {
            return doCreateSingleAggregator<KeyType,
                                            autil::MultiValueType<KeyType>,
                                            GroupDenseMapType >(aggDes);
        } else {
            return doCreateSingleAggregator<KeyType, KeyType,
                                            GroupDenseMapType>(aggDes);
        }
    } else {
        typedef std::tr1::unordered_map<KeyType, matchdoc::MatchDoc> GroupSparseMapType;
        if (groupKeyExpr->isMultiValue()) {
            return doCreateSingleAggregator<KeyType, autil::MultiValueType<KeyType>,
                                            GroupSparseMapType >(aggDes);
        } else {
            return doCreateSingleAggregator<KeyType, KeyType,
                                            GroupSparseMapType>(aggDes);
        }
    }

    return NULL;
}

template<typename KeyType, typename ExprType, typename GroupMapType>
Aggregator * AggregatorCreator::doCreateSingleAggregator(const AggregateDescription *aggDes) {
    HA3_LOG(TRACE3, "doCreateSingleAggregator");

    const SyntaxExpr *groupKeyExpr = aggDes->getGroupKeyExpr();
    AttributeExpression *attrExpr =
        _attrExprCreator->createAttributeExpression(groupKeyExpr);
    if (!attrExpr) {
        HA3_LOG(WARN, "create aggregate group key expression[%s] failed.",
                groupKeyExpr->getExprString().c_str());
        return NULL;
    }
    AttributeExpressionTyped<ExprType> *expr
        = dynamic_cast<AttributeExpressionTyped<ExprType> *>(attrExpr);
    if (!expr) {
        HA3_LOG(WARN, "unexpected group key type for aggregator[%s].",
                groupKeyExpr->getExprString().c_str());
        return NULL;
    }
    uint32_t maxGroupCount = aggDes->getMaxGroupCount();
    uint32_t aggThreshold = aggDes->getAggThreshold();
    uint32_t sampleStep = aggDes->getSampleStep();
    uint32_t maxSortCount = _aggSamplerConfigInfo.getMaxSortCount();
    bool aggBatchMode = _aggSamplerConfigInfo.getAggBatchMode();
    uint32_t batchSampleMaxCount =
        _aggSamplerConfigInfo.getBatchSampleMaxCount();
    if (aggThreshold == 0 && sampleStep == 0) {
        aggThreshold = _aggSamplerConfigInfo.getAggThreshold();
        sampleStep = _aggSamplerConfigInfo.getSampleStep();
    }

    Aggregator *aggregator = NULL;
    if (expr->getExpressionType() == ET_TUPLE) {
        assert(expr->getType() == vt_uint64);
        AttributeExpressionTyped<uint64_t> *tupleExpression =
            dynamic_cast<AttributeExpressionTyped<uint64_t> *>(expr);
        TupleAggregator *agg = new TupleAggregator(
                tupleExpression, maxGroupCount, _pool, aggThreshold,
                sampleStep, maxSortCount, _queryResource, _aggSamplerConfigInfo.getTupleSep());
        agg->setGroupKeyStr(groupKeyExpr->getExprString());
        aggregator = agg;
    } else if (aggDes->isRangeAggregate()) {
        //todo aggBatchMode not support rangeAgg
        const vector<string> &ranges = aggDes->getRange();
        vector<KeyType> typeRanges;
        autil::StringUtil::fromString(ranges, typeRanges);
        typedef RangeAggregator<KeyType, ExprType, GroupMapType> RangeAggregatorType;
        RangeAggregatorType *agg = new RangeAggregatorType(
                expr, typeRanges, _pool, maxGroupCount,
                aggThreshold, sampleStep, maxSortCount);
        agg->setGroupKeyStr(groupKeyExpr->getExprString());
        aggregator = agg;
    } else if (aggBatchMode){
        typedef BatchAggregator<KeyType, ExprType, GroupMapType> BatchAggregatorType;
        BatchAggregatorType *agg = new BatchAggregatorType(
                expr, maxGroupCount, _pool, aggThreshold,
                batchSampleMaxCount, maxSortCount, _queryResource);
        agg->setGroupKeyStr(groupKeyExpr->getExprString());
        aggregator = agg;
    } else {
        typedef NormalAggregator<KeyType, ExprType, GroupMapType> NormalAggregatorType;
        NormalAggregatorType *agg = new NormalAggregatorType(
                expr, maxGroupCount, _pool, aggThreshold,
                sampleStep, maxSortCount, _queryResource);
        agg->setGroupKeyStr(groupKeyExpr->getExprString());
        aggregator = agg;
    }
    // set aggregate filter
    Filter *aggFilter = createAggregateFilter(aggDes);
    FilterClause* filterClause = aggDes->getFilterClause();
    aggregator->setFilter(aggFilter);

    // create aggregate functions
    const vector<AggFunDescription*>&
        aggFuns = aggDes->getAggFunDescriptions();
    uint32_t functionCount = 0;
    for (vector<AggFunDescription*>::const_iterator it = aggFuns.begin();
         it != aggFuns.end(); it++)
    {
        const AggFunDescription *aggFunDes = (*it);
        AggregateFunction *aggFun = createAggregateFunction(aggFunDes);
        if(!aggFun) {
            HA3_LOG(WARN, "Create AggregateFunction error: %s", aggDes->getOriginalString().c_str());
            continue;
        }
        aggregator->addAggregateFunction(aggFun);
        functionCount++;
    }
    if (0 == functionCount
        || (NULL != filterClause && NULL == aggFilter)
        || aggFuns.size() != functionCount)
    {
        DELETE_AND_SET_NULL(aggregator);
    }
    typedef NormalAggregator<KeyType, ExprType, GroupMapType> NormalAggregatorType;
    auto normalAggregatorTyped = dynamic_cast<NormalAggregatorType *>(aggregator);
    if (_cavaAllocator && _aggSamplerConfigInfo.isEnableJit() && normalAggregatorTyped) {
        auto cavaAggJitModule = normalAggregatorTyped->codegen();
        if (cavaAggJitModule) {
            typedef JitNormalAggregator<KeyType, ExprType, GroupMapType> JitNormalAggregatorType;
            aggregator = new JitNormalAggregatorType(normalAggregatorTyped, _pool,
                    cavaAggJitModule, _cavaAllocator);
            REQUEST_TRACE(TRACE3, "create jit agg object for %s", aggDes->getOriginalString().c_str());
            return aggregator;
        }
    }
    REQUEST_TRACE(TRACE3, "no jit agg object created");
    return aggregator;
}

Aggregator* AggregatorCreator::createSingleAggregator(const AggregateDescription *aggDes) {
    Aggregator *aggregator = NULL;

    auto vt = aggDes->getGroupKeyExpr()->getExprResultType();

#define CREATE_SINGLE_AGG_DELEGATOR_HELPER(vt_type)                     \
    case vt_type:                                                       \
        {                                                               \
            typedef VariableTypeTraits<vt_type, false>::AttrExprType T; \
            aggregator = doCreateSingleAggregatorDelegator<T>(aggDes);  \
        }                                                               \
    break

    switch(vt) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL(CREATE_SINGLE_AGG_DELEGATOR_HELPER);
        CREATE_SINGLE_AGG_DELEGATOR_HELPER(vt_string);
    default:
        auto typeStr = vt2TypeName(vt);
        HA3_LOG(WARN, "does not support this Aggregator type: %s",
                typeStr.c_str());
        aggregator = NULL;
        stringstream ss;
        ss << "Aggregator type:" << typeStr.c_str();
        _errorResult.setErrorCode(ERROR_AGG_TYPE_NOT_SUPPORT);
        _errorResult.setErrorMsg(ss.str());
        break;
    }
#undef CREATE_SINGLE_AGG_DELEGATOR_HELPER
    return aggregator;
}

END_HA3_NAMESPACE(search);
