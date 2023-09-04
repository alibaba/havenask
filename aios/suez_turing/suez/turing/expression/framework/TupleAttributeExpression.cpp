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
#include "suez/turing/expression/framework/TupleAttributeExpression.h"

#include <cstddef>

using namespace std;

namespace suez {
namespace turing {

bool TupleAttributeExpression::operator==(const AttributeExpression *expr) const {
    auto checkExpr = dynamic_cast<const TupleAttributeExpression *>(expr);
    if (checkExpr != NULL) {
        if (_exprs.size() != checkExpr->_exprs.size()) {
            return false;
        }
        for (size_t i = 0; i < _exprs.size(); ++i) {
            if (!(*_exprs[i] == checkExpr->_exprs[i])) {
                return false;
            }
        }
        return true;
    }
    return false;
}

} // namespace turing
} // namespace suez
