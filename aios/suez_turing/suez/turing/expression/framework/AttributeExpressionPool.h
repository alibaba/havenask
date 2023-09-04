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
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "suez/turing/expression/cava/impl/AttributeExpression.h"
#include "suez/turing/expression/common.h"

namespace suez {
namespace turing {
class AttributeExpression;

class AttributeExpressionPool {
public:
    AttributeExpressionPool();
    ~AttributeExpressionPool();

private:
    AttributeExpressionPool(const AttributeExpressionPool &);
    AttributeExpressionPool &operator=(const AttributeExpressionPool &);

public:
    inline AttributeExpression *tryGetAttributeExpression(const std::string &exprStr) const {
        std::map<std::string, AttributeExpression *>::const_iterator it = _exprMap.find(exprStr);
        if (it != _exprMap.end()) {
            return it->second;
        }
        return NULL;
    }

    void addPair(const std::string &exprStr, AttributeExpression *attrExpr, bool needDelete = true);
    void addNeedDeleteExpr(AttributeExpression *attrExpr);

private:
    bool assertNoDupExpression();

private:
    std::vector<AttributeExpression *> _exprVecs;
    std::map<std::string, AttributeExpression *> _exprMap;

private:
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(AttributeExpressionPool);

} // namespace turing
} // namespace suez
