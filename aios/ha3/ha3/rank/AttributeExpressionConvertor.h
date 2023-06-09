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

#include <assert.h>
#include <stddef.h>
#include <memory>

#include "alog/Logger.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "matchdoc/Reference.h"
#include "matchdoc/ValueType.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/ComboAttributeExpression.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"

namespace isearch {
namespace rank {

class AttributeExpressionConvertor
{
public:
    AttributeExpressionConvertor();
    ~AttributeExpressionConvertor();
private:
    AttributeExpressionConvertor(const AttributeExpressionConvertor &);
    AttributeExpressionConvertor& operator=(const AttributeExpressionConvertor &);
public:
    template<class BaseType, template <typename T> class DerivedType>
    static BaseType* convert(suez::turing::AttributeExpression* attrExpr) {
        if (NULL == attrExpr) {
            AUTIL_LOG(DEBUG, "attr expr is NULL");
            return NULL;
        }

        matchdoc::ReferenceBase *ref = NULL;
        if (suez::turing::ET_COMBO != attrExpr->getExpressionType()) {
            ref = attrExpr->getReferenceBase();
        } else {
            suez::turing::ComboAttributeExpression *comboAttrExpr =
                dynamic_cast<suez::turing::ComboAttributeExpression*>(attrExpr);
            assert(comboAttrExpr);
            const suez::turing::ExpressionVector &exprVect = comboAttrExpr->getExpressions();
            if (exprVect.size() == 0) {
                AUTIL_LOG(DEBUG, "combo attr expr vect size is 0");
                return NULL;
            }
            ref = exprVect[0]->getReferenceBase();
        }

        if (ref == NULL) {
            AUTIL_LOG(WARN, "ref of attrexpr is NULL");
            return NULL;
        }

        auto vt = ref->getValueType().getBuiltinType();
        AUTIL_LOG(DEBUG, "variable type:%d", (int)vt);
#ifndef CREATE_DERIVED_OBJECT_CASE
#define CREATE_DERIVED_OBJECT_CASE(vt)                                  \
        case vt:                                                        \
        {                                                               \
            auto refTyped = dynamic_cast<matchdoc::Reference<           \
                suez::turing::VariableTypeTraits<vt, false>::AttrExprType> *>(ref); \
            if (refTyped == NULL) {                                     \
                AUTIL_LOG(WARN, "dynamic varref cast failed");            \
                return NULL;                                            \
            }                                                           \
            return new DerivedType<                                     \
                suez::turing::VariableTypeTraits<vt, false>::AttrExprType>(refTyped); \
        }

        switch(vt) {
            CREATE_DERIVED_OBJECT_CASE(vt_int8);
            CREATE_DERIVED_OBJECT_CASE(vt_int16);
            CREATE_DERIVED_OBJECT_CASE(vt_int32);
            CREATE_DERIVED_OBJECT_CASE(vt_int64);
            CREATE_DERIVED_OBJECT_CASE(vt_uint8);
            CREATE_DERIVED_OBJECT_CASE(vt_uint16);
            CREATE_DERIVED_OBJECT_CASE(vt_uint32);
            CREATE_DERIVED_OBJECT_CASE(vt_uint64);
            CREATE_DERIVED_OBJECT_CASE(vt_float);
            CREATE_DERIVED_OBJECT_CASE(vt_double);
        default:
            AUTIL_LOG(WARN, "invalid variable type");
            return NULL;
        }
#undef CREATE_DERIVED_OBJECT_CASE
#endif
    }
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<AttributeExpressionConvertor> AttributeExpressionConvertorPtr;

} // namespace rank
} // namespace isearch
