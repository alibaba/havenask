#ifndef ISEARCH_INNERRESULTVARIANT_H
#define ISEARCH_INNERRESULTVARIANT_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/InnerSearchResult.h>
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/variant_encode_decode.h"

BEGIN_HA3_NAMESPACE(turing);

class InnerResultVariant
{
public:
    InnerResultVariant()
        : _innerResult(nullptr)
    {}
    InnerResultVariant(const HA3_NS(search)::InnerSearchResult &innerResult)
        : _innerResult(innerResult)
    {
    }
    ~InnerResultVariant() {}

public:
    void Encode(tensorflow::VariantTensorData* data) const {}
    bool Decode(const tensorflow::VariantTensorData& data) {
        return false;
    }
    std::string TypeName() const {
        return "InnerResult";
    }
public:
    HA3_NS(search)::InnerSearchResult getInnerResult() const {
        return _innerResult;
    }
private:
    HA3_NS(search)::InnerSearchResult _innerResult;
private:
    HA3_LOG_DECLARE();

};

HA3_TYPEDEF_PTR(InnerResultVariant);

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_INNERRESULTVARIANT_H
