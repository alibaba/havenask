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

#include <stddef.h>
#include <string>
#include <vector>

#include "suez/turing/expression/common.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"

namespace autil {
class DataBuffer;
} // namespace autil

namespace suez {
namespace turing {
class SyntaxExprVisitor;

class MultiSyntaxExpr : public SyntaxExpr {
public:
    MultiSyntaxExpr() : SyntaxExpr(vt_uint64, SYNTAX_EXPR_TYPE_MULTI) {}
    ~MultiSyntaxExpr() {
        for (auto expr : _exprs) {
            delete expr;
        }
        _exprs.clear();
    }
    void addSyntaxExpr(SyntaxExpr *expr) {
        _exprs.push_back(expr);
        SyntaxExpr::setExprString(getExprString());
    }
    const std::vector<SyntaxExpr *> &getSyntaxExprs() const { return _exprs; }
    bool operator==(const SyntaxExpr *expr) const override;
    void accept(SyntaxExprVisitor *visitor) const override;
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;

public:
    std::string getExprString() const override {
        std::string ret = "[";
        for (size_t i = 0; i < _exprs.size(); i++) {
            if (i != 0) {
                ret += ", ";
            }
            ret += _exprs[i]->getExprString();
        }
        ret += "]";
        return ret;
    }

private:
    std::vector<SyntaxExpr *> _exprs;
};

} // namespace turing
} // namespace suez
