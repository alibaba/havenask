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
#include "indexlib/index/attribute/expression/DocumentEvaluatorMaintainer.h"

#include "expression/framework/AttributeExpressionCreator.h"
#include "expression/function/FunctionInterfaceManager.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/expression/AtomicExpressionCreator.h"
#include "indexlib/index/attribute/expression/BuiltinFunctionFactory.h"
#include "indexlib/index/attribute/expression/DocumentEvaluator.h"
#include "indexlib/index/attribute/expression/FunctionConfig.h"
#include "indexlib/index/attribute/expression/TabletSessionResource.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, DocumentEvaluatorMaintainer);

DocumentEvaluatorMaintainer::DocumentEvaluatorMaintainer() { _pool.reset(new autil::mem_pool::Pool); }

DocumentEvaluatorMaintainer::~DocumentEvaluatorMaintainer()
{
    if (_attributeExpressionCreator) {
        _attributeExpressionCreator->endRequest();
    }
}

Status DocumentEvaluatorMaintainer::Init(const std::vector<std::shared_ptr<framework::Segment>>& segments,
                                         const std::shared_ptr<config::TabletSchema>& schema,
                                         const std::vector<std::string>& functionNames)
{
    AUTIL_LOG(INFO, "init for functions [%s] begin", autil::legacy::ToJsonString(functionNames, true).c_str());
    _evaluators.clear();
    _schema = schema;
    _sessionResource.reset(new TabletSessionResource(_pool.get(), segments));
    auto creatorFunction = [this, segments, schema](expression::AttributeExpressionPool* exprPool,
                                                    autil::mem_pool::Pool* pool,
                                                    bool useSub) -> expression::AtomicAttributeExpressionCreatorBase* {
        AtomicExpressionCreator* creator = new AtomicExpressionCreator(segments, schema, exprPool, pool);
        _atomicExpressionCreator = creator;
        return creator;
    };
    _functionInterfaceFactory = std::make_unique<BuiltinFunctionFactory>();
    _functionInterfaceManager = std::make_unique<expression::FunctionInterfaceManager>();
    if (!_functionInterfaceManager->init(expression::FunctionConfig(), nullptr, _functionInterfaceFactory.get())) {
        AUTIL_LOG(ERROR, "init function interface manager failed");
        return Status::InvalidArgs("init function interface manager failed");
    }
    _attributeExpressionCreator.reset(
        new expression::AttributeExpressionCreator(_functionInterfaceManager.get(), false, creatorFunction));
    for (const auto& functionName : functionNames) {
        auto evaluator = CreateEvaluator(functionName);
        if (!evaluator) {
            AUTIL_LOG(ERROR, "create evaluator for function [%s] failed", functionName.c_str());
            _attributeExpressionCreator.reset();
            return Status::InvalidArgs("create evaluator failed");
        }
        _evaluators.push_back(evaluator);
    }
    if (!_attributeExpressionCreator->beginRequest(_sessionResource.get())) {
        AUTIL_LOG(ERROR, "begin request failed");
        return Status::InvalidArgs("begin request failed");
    }
    _sessionResource->allocator->extend();
    AUTIL_LOG(INFO, "init for functions [%s] success", autil::legacy::ToJsonString(functionNames, true).c_str());
    return Status::OK();
}

std::shared_ptr<DocumentEvaluatorBase> DocumentEvaluatorMaintainer::CreateEvaluator(const std::string& functionName)
{
    auto [status, attributeFunctionConfig] =
        _schema->GetSetting<AttributeFunctionConfig>(ATTRIBUTE_FUNCTION_CONFIG_KEY);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "get function configs failed");
        return nullptr;
    }
    const auto& functionConfig = attributeFunctionConfig.GetFunctionConfig(functionName);
    if (functionConfig == nullptr) {
        AUTIL_LOG(ERROR, "no match function [%s], function configs are [%s]", functionName.c_str(),
                  autil::legacy::ToJsonString(attributeFunctionConfig, true).c_str());
        return nullptr;
    }
    return InnerCreateEvaluator(functionConfig->GetExpression());
}

std::shared_ptr<DocumentEvaluatorBase>
DocumentEvaluatorMaintainer::InnerCreateEvaluator(const std::string& expressionStr)
{
    using namespace expression;
    size_t beforeCreateExpressionCount = _atomicExpressionCreator->GetCreatedExpressionCount();
    expression::AttributeExpression* expression = _attributeExpressionCreator->create(expressionStr);
    if (!expression) {
        return nullptr;
    }
    size_t afterCreateExpressionCount = _atomicExpressionCreator->GetCreatedExpressionCount();
    std::vector<AtomicAttributeExpressionBase*> dependAtomicExpressions;
    for (size_t idx = beforeCreateExpressionCount; idx < afterCreateExpressionCount; ++idx) {
        dependAtomicExpressions.push_back(_atomicExpressionCreator->GetCreatedExpression(idx));
    }

    std::shared_ptr<DocumentEvaluatorBase> evaluator;

#define SET_ATTR_ITERATOR_TYPED(evt_)                                                                                  \
    case evt_:                                                                                                         \
        if (expression->isMulti()) {                                                                                   \
            typedef ExprValueType2AttrValueType<evt_, true>::AttrValueType T;                                          \
            evaluator.reset(new DocumentEvaluator<T>());                                                               \
        } else {                                                                                                       \
            typedef ExprValueType2AttrValueType<evt_, false>::AttrValueType T;                                         \
            evaluator.reset(new DocumentEvaluator<T>());                                                               \
        }                                                                                                              \
        break

    switch (expression->getExprValueType()) {
        EXPR_VALUE_TYPE_MACRO_HELPER(SET_ATTR_ITERATOR_TYPED);
    default:
        AUTIL_LOG(WARN, "unsupport variable type[%d] for attribute", (int32_t)expression->getExprValueType());
        break;
    }
#undef SET_ATTR_ITERATOR_TYPED
    if (evaluator) {
        if (!evaluator->Init(dependAtomicExpressions, _sessionResource->allocator, expression).IsOK()) {
            AUTIL_LOG(ERROR, "init evaluator failed");
            evaluator.reset();
        }
    }
    return evaluator;
}

std::vector<std::shared_ptr<DocumentEvaluatorBase>> DocumentEvaluatorMaintainer::GetAllEvaluators()
{
    return _evaluators;
}
} // namespace indexlibv2::index
