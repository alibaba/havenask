/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Request.h"
#include "matchdoc/MatchDocAllocator.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/provider/FunctionProvider.h"
#include "tensorflow/core/framework/variant_tensor_data.h"

namespace isearch {
namespace turing {

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

typedef std::shared_ptr<ExpressionResource> ExpressionResourcePtr;

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

typedef std::shared_ptr<ExpressionResourceVariant> ExpressionResourceVariantPtr;

} // namespace turing
} // namespace isearch
