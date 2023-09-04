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

#include "suez/turing/expression/framework/MatchDocsExpressionCreator.h"

#include <cstddef>
#include <map>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "matchdoc/FieldGroup.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "matchdoc/ValueType.h"
#include "suez/turing/expression/framework/MatchDocsSyntaxExpr2AttrExpr.h"
#include "suez/turing/expression/function/FunctionInterfaceCreator.h"
#include "suez/turing/expression/provider/FunctionProvider.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"
#include "suez/turing/expression/syntax/SyntaxExprValidator.h"
#include "suez/turing/expression/util/FieldInfos.h"
#include "suez/turing/expression/util/TypeTransformer.h"
#include "suez/turing/expression/util/VirtualAttribute.h"
#include "turing_ops_util/variant/Tracer.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace suez {
namespace turing {
class AttributeExpression;
class CavaPluginManager;
class FunctionProvider;
class SuezCavaAllocator;
} // namespace turing
} // namespace suez

using namespace std;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, MatchDocsExpressionCreator);

MatchDocsExpressionCreator::MatchDocsExpressionCreator(autil::mem_pool::Pool *pool,
                                                       matchdoc::MatchDocAllocator *matchAllocator,
                                                       const FunctionInterfaceCreator *funcCreator,
                                                       const suez::turing::CavaPluginManager *cavaPluginManager,
                                                       suez::turing::FunctionProvider *functionProvider,
                                                       suez::turing::SuezCavaAllocator *cavaAllocator,
                                                       const map<string, string> *fieldMapping)
    : AttributeExpressionCreatorBase(pool)
    , _allocator(matchAllocator)
    , _funcManager(funcCreator, functionProvider, pool, cavaPluginManager)
    , _fieldMapping(fieldMapping)
    , _tracer(functionProvider ? functionProvider->getRequestTracer() : nullptr) {
    fillAttributeInfos();
    bool hasSub = _allocator ? _allocator->hasSubDocAllocator() : false;
    _syntaxExprValidator = new SyntaxExprValidator(&_attributeInfos, VirtualAttributes(), hasSub);
    _syntaxExprValidator->addFuncInfoMap(&funcCreator->getFuncInfoMap());
    if (cavaPluginManager) {
        _syntaxExprValidator->addCavaPluginManager(cavaPluginManager);
    }
}

MatchDocsExpressionCreator::~MatchDocsExpressionCreator() { DELETE_AND_SET_NULL(_syntaxExprValidator); }

AttributeExpression *MatchDocsExpressionCreator::createAttributeExpression(const SyntaxExpr *syntaxExpr,
                                                                           bool needValidate) {
    if (_allocator == nullptr) {
        return NULL;
    }
    if (needValidate) {
        if (!_syntaxExprValidator->validate(syntaxExpr)) {
            TURING_LOG_AND_TRACE(WARN,
                                 "validate attribute expression [%s] fail, error msg [%s]",
                                 syntaxExpr->getExprString().c_str(),
                                 _syntaxExprValidator->getErrorMsg().c_str());
            return NULL;
        }
    }
    MatchDocsSyntaxExpr2AttrExpr visitor(_allocator, &_funcManager, _exprPool, _pool, _fieldMapping);
    syntaxExpr->accept(&visitor);
    AttributeExpression *ret = visitor.stealAttrExpr();
    if (!ret) {
        TURING_LOG_AND_TRACE(WARN, "create attribute expression [%s] fail", syntaxExpr->getExprString().c_str());
        return NULL;
    }
    return ret;
}

void MatchDocsExpressionCreator::fillAttributeInfos() {
    if (_allocator == nullptr) {
        return;
    }
    matchdoc::ReferenceNameSet fields = _allocator->getAllReferenceNames();
    auto aliasMap = _allocator->getReferenceAliasMap();
    if (aliasMap) {
        matchdoc::MatchDocAllocator::ReferenceAliasMap::const_iterator iter = aliasMap->begin();
        for (; iter != aliasMap->end(); ++iter) {
            fields.insert(iter->first);
        }
    }
    for (auto &field : fields) {
        matchdoc::ReferenceBase *ref = _allocator->findReferenceWithoutType(field);
        if (nullptr == ref) {
            continue;
        }
        matchdoc::ValueType vt = ref->getValueType();
        if (!vt.isBuiltInType()) {
            continue;
        }
        FieldInfo fieldInfo(field, TypeTransformer::transform(vt.getBuiltinType()), vt.isMultiValue());
        AttributeInfo *attrInfo =
            new AttributeInfo(fieldInfo.fieldName, fieldInfo, vt, false, fieldInfo.isMultiValue, false);
        _attributeInfos.addAttributeInfo(attrInfo);
    }
}

} // namespace turing
} // namespace suez
