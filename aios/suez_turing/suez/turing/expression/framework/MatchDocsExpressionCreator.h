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

#include <map>
#include <stddef.h>
#include <string>

#include "autil/Log.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpressionCreatorBase.h"
#include "suez/turing/expression/function/FunctionManager.h"
#include "suez/turing/expression/util/AttributeInfos.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace matchdoc {
class MatchDocAllocator;
} // namespace matchdoc

namespace suez {
namespace turing {
class AttributeExpression;
class FunctionInterfaceCreator;
class FunctionProvider;
class SuezCavaAllocator;
class SyntaxExpr;
class SyntaxExprValidator;
class Tracer;

class MatchDocsExpressionCreator : public AttributeExpressionCreatorBase {
public:
    MatchDocsExpressionCreator(autil::mem_pool::Pool *pool,
                               matchdoc::MatchDocAllocator *matchDocAllocator,
                               const FunctionInterfaceCreator *funcCreator,
                               const suez::turing::CavaPluginManager *cavaPluginManager,
                               suez::turing::FunctionProvider *functionProvider,
                               suez::turing::SuezCavaAllocator *cavaAllocator,
                               const std::map<std::string, std::string> *fieldMapping = nullptr);
    ~MatchDocsExpressionCreator();

private:
    MatchDocsExpressionCreator(const MatchDocsExpressionCreator &);
    MatchDocsExpressionCreator &operator=(const MatchDocsExpressionCreator &);

public:
    using AttributeExpressionCreatorBase::createAttributeExpression;
    AttributeExpression *createAtomicExpr(const std::string &attrName) override { return NULL; }

    AttributeExpression *createAttributeExpression(const SyntaxExpr *syntaxExpr, bool needValidate = false) override;

private:
    void fillAttributeInfos();

private:
    AttributeInfos _attributeInfos;
    SyntaxExprValidator *_syntaxExprValidator;
    matchdoc::MatchDocAllocator *_allocator;
    FunctionManager _funcManager;
    const std::map<std::string, std::string> *_fieldMapping;
    suez::turing::Tracer *_tracer = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(MatchDocsExpressionCreator);

} // namespace turing
} // namespace suez
