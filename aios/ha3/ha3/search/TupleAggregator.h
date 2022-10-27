#ifndef ISEARCH_TUPLEAGGREGATOR_H
#define ISEARCH_TUPLEAGGREGATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/NormalAggregator.h>
#include <unordered_map>
#include <suez/turing/expression/framework/TupleAttributeExpression.h>

BEGIN_HA3_NAMESPACE(search);
typedef typename UnorderedMapTraits<uint64_t>::GroupMapType TupleAggregatorGroupMapType;
class TupleAggregator : public NormalAggregator<uint64_t, uint64_t, TupleAggregatorGroupMapType>
{
public:
    TupleAggregator(suez::turing::AttributeExpressionTyped<uint64_t> *attriExpr,
                    uint32_t maxKeyCount,
                    autil::mem_pool::Pool *pool,
                    uint32_t aggThreshold = 0,
                    uint32_t sampleStep = 1,
                    uint32_t maxSortCount = 0,
                    tensorflow::QueryResource *queryResource = NULL,
                    const std::string &sep = "|");
    ~TupleAggregator();
private:
    TupleAggregator(const TupleAggregator &);
    TupleAggregator& operator=(const TupleAggregator &);
public:
    void initGroupKeyRef() override;
    void setGroupKey(matchdoc::MatchDoc aggMatchDoc,
                     const uint64_t &groupKeyValue) override;
    suez::turing::CavaAggModuleInfoPtr codegen() override {
        return suez::turing::CavaAggModuleInfoPtr();
    }
private:
    suez::turing::TupleAttributeExpression *_tupleAttributeExpression;
    matchdoc::Reference<std::string> *_groupKeyRef;
    std::string _sep;
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(TupleAggregator);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_TUPLEAGGREGATOR_H
