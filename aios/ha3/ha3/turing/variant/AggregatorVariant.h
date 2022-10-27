#ifndef ISEARCH_AGGREGATORVARIANT_H
#define ISEARCH_AGGREGATORVARIANT_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/Aggregator.h>
#include <tensorflow/core/framework/variant.h>
#include <tensorflow/core/framework/tensor.h>
#include <tensorflow/core/framework/variant_encode_decode.h>
#include <ha3/turing/variant/ExpressionResourceVariant.h>

BEGIN_HA3_NAMESPACE(turing);

class AggregatorVariant
{
public:
    AggregatorVariant()
        : _seekedCount(0)
        , _seekTermDocCount(0)
    {}
    AggregatorVariant(const search::AggregatorPtr &aggregator,
                      const ExpressionResourcePtr &resource)
        : _aggregator(aggregator)
        , _expressionResource(resource)
        , _seekedCount(0)
        , _seekTermDocCount(0)
    {}
    ~AggregatorVariant() {}
public:
    void Encode(tensorflow::VariantTensorData* data) const {}
    bool Decode(const tensorflow::VariantTensorData& data) {
        return false;
    }
    std::string TypeName() const {
        return "Aggregator";
    }

    uint64_t getSeekedCount() const { return _seekedCount; }
    uint64_t getSeekTermDocCount() const { return _seekTermDocCount; }

    void setSeekedCount(uint64_t seekedCount) {
        _seekedCount = seekedCount;
    }
    void setSeekTermDocCount(uint64_t seekTermDocCount) {
        _seekTermDocCount = seekTermDocCount;
    }

public:
    search::AggregatorPtr getAggregator() const {
        return _aggregator;
    }
    ExpressionResourcePtr getExpressionResource() const {
        return _expressionResource;
    }
private:
    search::AggregatorPtr _aggregator;
    ExpressionResourcePtr _expressionResource;
    uint64_t _seekedCount;
    uint64_t _seekTermDocCount;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_AGGREGATORVARIANT_H
