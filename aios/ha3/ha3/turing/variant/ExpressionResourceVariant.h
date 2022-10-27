#ifndef ISEARCH_EXPRESSIONRESOURCEVARIANT_H
#define ISEARCH_EXPRESSIONRESOURCEVARIANT_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <suez/turing/expression/provider/FunctionProvider.h>
#include <matchdoc/MatchDocAllocator.h>
#include <ha3/common/Request.h>
#include <tensorflow/core/framework/variant.h>
#include <tensorflow/core/framework/tensor.h>
#include <tensorflow/core/framework/variant_encode_decode.h>

BEGIN_HA3_NAMESPACE(turing);

struct ExpressionResource {
public:
    ExpressionResource() {}
    ExpressionResource(const common::RequestPtr &request,
                       const matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
                       const suez::turing::FunctionProviderPtr &functionProvider,
                       const suez::turing::AttributeExpressionCreatorPtr &attributeExpressionCreator)
        : _request(request)
        , _matchDocAllocator(matchDocAllocator)
        , _functionProvider(functionProvider)
        , _attributeExpressionCreator(attributeExpressionCreator)
    {}
public:
    common::RequestPtr _request; // reserve kvpair, virtual attribute reference
    matchdoc::MatchDocAllocatorPtr _matchDocAllocator;
    suez::turing::FunctionProviderPtr _functionProvider;
    suez::turing::AttributeExpressionCreatorPtr _attributeExpressionCreator;
};

HA3_TYPEDEF_PTR(ExpressionResource);

class ExpressionResourceVariant
{
public:
    ExpressionResourceVariant() {}
    ExpressionResourceVariant(const ExpressionResourcePtr &resource)
        : _expressionResource(resource)
    {}
    ~ExpressionResourceVariant() {}
public:
    void Encode(tensorflow::VariantTensorData* data) const {}
    bool Decode(const tensorflow::VariantTensorData& data) {
        return false;
    }
    std::string TypeName() const {
        return "ExpressionResource";
    }
public:
    ExpressionResourcePtr getExpressionResource() const {
        return _expressionResource;
    }
private:
    ExpressionResourcePtr _expressionResource;
};

HA3_TYPEDEF_PTR(ExpressionResourceVariant);

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_EXPRESSIONRESOURCEVARIANT_H
