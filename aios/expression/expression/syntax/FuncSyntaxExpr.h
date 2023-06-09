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
#ifndef ISEARCH_EXPRESSION_FUNCSYNTAXEXPR_H
#define ISEARCH_EXPRESSION_FUNCSYNTAXEXPR_H

#include "expression/common.h"
#include "expression/syntax/SyntaxExpr.h"

namespace expression {

class FuncSyntaxExpr : public SyntaxExpr
{
public:
    typedef std::vector<SyntaxExpr *> SubExprType;
public:
    FuncSyntaxExpr(const std::string &funcName = std::string(),
                   const SubExprType &params = SubExprType(),
                   autil::mem_pool::Pool* pool = NULL);
    
    ~FuncSyntaxExpr();
public:
    /* override */ bool operator == (const SyntaxExpr *expr) const;
    /* override */ void accept(SyntaxExprVisitor *visitor) const;

    /* override */ void serialize(autil::DataBuffer &dataBuffer) const;
    /* override */ void deserialize(autil::DataBuffer &dataBuffer);
public:
    const SubExprType &getArgumentSyntaxExprs() const { return _exprs; }
    const std::string &getFunctionName() const { return _funcName; }
private:
    void initExprString() {
        if (_pool) {
            SyntaxExpr::setExprString(genExprConstString());
        } else {
            SyntaxExpr::setExprString(genExprString());
        }
    }
    
    std::string genExprString() const;
    autil::StringView genExprConstString() const;
protected:
    std::string _funcName;
    SubExprType _exprs;
private:
    AUTIL_LOG_DECLARE();
};

EXPRESSION_TYPEDEF_PTR(FuncSyntaxExpr);

}

#endif //ISEARCH_EXPRESSION_FUNCSYNTAXEXPR_H
