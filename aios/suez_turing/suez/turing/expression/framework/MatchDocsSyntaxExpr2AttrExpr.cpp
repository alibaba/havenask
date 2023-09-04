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
#include "suez/turing/expression/framework/MatchDocsSyntaxExpr2AttrExpr.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <map>
#include <string>
#include <utility>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/MultiValueType.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "matchdoc/ValueType.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/AttributeExpressionPool.h"
#include "suez/turing/expression/framework/SyntaxExpr2AttrExpr.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"

namespace suez {
namespace turing {
class FunctionManager;
class RankSyntaxExpr;
} // namespace turing
} // namespace suez

using namespace std;
using namespace autil;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, MatchDocsSyntaxExpr2AttrExpr);

MatchDocsSyntaxExpr2AttrExpr::MatchDocsSyntaxExpr2AttrExpr(matchdoc::MatchDocAllocator *allocator,
                                                           FunctionManager *funcManager,
                                                           AttributeExpressionPool *exprPool,
                                                           autil::mem_pool::Pool *pool,
                                                           const map<string, string> *fieldMapping)
    : SyntaxExpr2AttrExpr(NULL, funcManager, exprPool, NULL, pool)
    , _allocator(allocator)
    , _fieldMapping(fieldMapping) {}

MatchDocsSyntaxExpr2AttrExpr::~MatchDocsSyntaxExpr2AttrExpr() {}

void MatchDocsSyntaxExpr2AttrExpr::visitRankSyntaxExpr(const RankSyntaxExpr *syntaxExpr) {
    AUTIL_LOG(TRACE3, "BEGIN visit SyntaxExpr");
    assert(false);
}

AttributeExpression *MatchDocsSyntaxExpr2AttrExpr::createAtomicExpr(const string &attrName,
                                                                    const string &prefixName,
                                                                    const string &completeName) {
    string realName = _fieldMapping ? _fieldMapping->at(attrName) : attrName;
    AttributeExpression *attrExpr = _exprPool->tryGetAttributeExpression(realName);
    if (attrExpr) {
        return attrExpr;
    }
    matchdoc::ReferenceBase *ref = _allocator->findReferenceWithoutType(realName);
    if (ref == NULL) {
        return NULL;
    }
    AttributeExpression *lastExpr = _attrExpr;
    _attrExpr = createAttributeExpression(ref, _pool);
    if (_attrExpr) {
        _attrExpr->setOriginalString(realName);
        _exprPool->addPair(realName, _attrExpr);
    }
    swap(_attrExpr, lastExpr);
    return lastExpr;
}

template <typename T>
AttributeExpression *MatchDocsSyntaxExpr2AttrExpr::doCreateAttributeExpression(matchdoc::ReferenceBase *refer,
                                                                               autil::mem_pool::Pool *pool) {
    auto vr = dynamic_cast<matchdoc::Reference<T> *>(refer);
    if (nullptr == vr) {
        AUTIL_LOG(WARN, "cast ref[%s] to typed ref failed", refer->getName().c_str());
        return nullptr;
    }
    auto expr = POOL_NEW_CLASS(pool, AttributeExpressionTyped<T>);
    expr->setReference(vr);
    return expr;
}

AttributeExpression *MatchDocsSyntaxExpr2AttrExpr::createAttributeExpression(matchdoc::ReferenceBase *refer,
                                                                             autil::mem_pool::Pool *pool) {
    auto valueType = refer->getValueType();
    auto vt = valueType.getBuiltinType();
    auto isMulti = valueType.isMultiValue();

#define CREATE_ATTRIBUTE_EXPRESSION_HELPER(vt_type)                                                                    \
    case vt_type: {                                                                                                    \
        if (isMulti) {                                                                                                 \
            typedef VariableTypeTraits<vt_type, true>::AttrExprType T;                                                 \
            return doCreateAttributeExpression<T>(refer, pool);                                                        \
        } else {                                                                                                       \
            typedef VariableTypeTraits<vt_type, false>::AttrExprType T;                                                \
            return doCreateAttributeExpression<T>(refer, pool);                                                        \
        }                                                                                                              \
    } break

    switch (vt) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL(CREATE_ATTRIBUTE_EXPRESSION_HELPER);
    case vt_string: {
        if (valueType.isStdType()) {
            AUTIL_LOG(WARN, "unsupport support std string");
            return NULL;
        }
        if (isMulti) {
            return doCreateAttributeExpression<autil::MultiString>(refer, pool);
        } else {
            return doCreateAttributeExpression<autil::MultiChar>(refer, pool);
        }
    }
    default:
        assert(false);
        break;
    }
    return NULL;
}
} // namespace turing
} // namespace suez
