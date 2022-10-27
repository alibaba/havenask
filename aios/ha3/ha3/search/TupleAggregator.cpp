#include <ha3/search/TupleAggregator.h>
#include <suez/turing/expression/framework/TupleAttributeExpression.h>

using namespace std;

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, TupleAggregator);

TupleAggregator::TupleAggregator(suez::turing::AttributeExpressionTyped<uint64_t> *attriExpr,
                                 uint32_t maxKeyCount,
                                 autil::mem_pool::Pool *pool,
                                 uint32_t aggThreshold,
                                 uint32_t sampleStep,
                                 uint32_t maxSortCount,
                                 tensorflow::QueryResource *queryResource,
                                 const std::string &sep)
    : NormalAggregator<uint64_t, uint64_t, TupleAggregatorGroupMapType>(attriExpr, maxKeyCount, pool,
            aggThreshold, sampleStep, maxSortCount, queryResource)
{
    _tupleAttributeExpression = dynamic_cast<suez::turing::TupleAttributeExpression *>(_expr);
    assert(_tupleAttributeExpression != NULL);
    _groupKeyRef = NULL;
    _sep = sep;
}

TupleAggregator::~TupleAggregator() {
}

void TupleAggregator::initGroupKeyRef() {
    _groupKeyWriter.initGroupKeyRef<std::string>(_aggMatchDocAllocatorPtr);
    _groupKeyRef = static_cast<matchdoc::Reference<std::string> *>(_groupKeyWriter.getGroupKeyRef());
}

void TupleAggregator::setGroupKey(matchdoc::MatchDoc aggMatchDoc,
                                  const uint64_t &groupKeyValue)
{
#define CONSTRUCT_ORIGINAL_MULTI_GROUP_KEY(type, actual_type)           \
    case type: {                                                        \
        auto attributeTyped = (suez::turing::AttributeExpressionTyped<actual_type> *)expr; \
        originalGroupKey += std::to_string(attributeTyped->getValue()); \
        break;                                                          \
    }

    std::string &originalGroupKey = _groupKeyRef->getReference(aggMatchDoc);
    auto &exprs = _tupleAttributeExpression->getAttributeExpressions();
    for (size_t i = 0; i < exprs.size(); ++i) {
        if (i != 0) {
            originalGroupKey += _sep;
        }
        auto expr = exprs[i];
        auto vt = expr->getType();
        switch (vt) {
            CONSTRUCT_ORIGINAL_MULTI_GROUP_KEY(vt_bool, bool);
            CONSTRUCT_ORIGINAL_MULTI_GROUP_KEY(vt_int8, int8_t);
            CONSTRUCT_ORIGINAL_MULTI_GROUP_KEY(vt_int16, int16_t);
            CONSTRUCT_ORIGINAL_MULTI_GROUP_KEY(vt_int32, int32_t);
            CONSTRUCT_ORIGINAL_MULTI_GROUP_KEY(vt_int64, int64_t);
            CONSTRUCT_ORIGINAL_MULTI_GROUP_KEY(vt_uint8, uint8_t);
            CONSTRUCT_ORIGINAL_MULTI_GROUP_KEY(vt_uint16, uint16_t);
            CONSTRUCT_ORIGINAL_MULTI_GROUP_KEY(vt_uint32, uint32_t);
            CONSTRUCT_ORIGINAL_MULTI_GROUP_KEY(vt_uint64, uint64_t);
            CONSTRUCT_ORIGINAL_MULTI_GROUP_KEY(vt_float, float);
            CONSTRUCT_ORIGINAL_MULTI_GROUP_KEY(vt_double, double);
        case vt_string: {
            auto attributeTyped = (suez::turing::AttributeExpressionTyped<autil::MultiChar> *)expr;
            auto data = attributeTyped->getValue().data();
            auto size = attributeTyped->getValue().size();
            originalGroupKey.append(data, size);
            break;
        }
        default:
            assert(false);
            break;
        }
    }
#undef CONSTRUCT_ORIGINAL_MULTI_GROUP_KEY
}

END_HA3_NAMESPACE(search);
