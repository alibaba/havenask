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
#include "suez/turing/expression/syntax/MultiSyntaxExpr.h"

#include <cstddef>

#include "autil/DataBuffer.h"
#include "suez/turing/expression/syntax/SyntaxExprVisitor.h"

using namespace std;

namespace suez {
namespace turing {

bool MultiSyntaxExpr::operator==(const SyntaxExpr *expr) const {
    const MultiSyntaxExpr *checkExpr = dynamic_cast<const MultiSyntaxExpr *>(expr);
    if (checkExpr) {
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

void MultiSyntaxExpr::accept(SyntaxExprVisitor *visitor) const { visitor->visitMultiSyntaxExpr(this); }

void MultiSyntaxExpr::serialize(autil::DataBuffer &dataBuffer) const {
    SyntaxExpr::serialize(dataBuffer);
    dataBuffer.write(_exprs);
}

void MultiSyntaxExpr::deserialize(autil::DataBuffer &dataBuffer) {
    SyntaxExpr::deserialize(dataBuffer);
    dataBuffer.read(_exprs);
}

} // namespace turing
} // namespace suez
