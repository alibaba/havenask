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
#ifndef ISEARCH_EXPRESSION_CONSTSYNTAXEXPR_H
#define ISEARCH_EXPRESSION_CONSTSYNTAXEXPR_H

#include "expression/common.h"
#include "expression/syntax/SyntaxExpr.h"

namespace expression {

class ConstSyntaxExpr : public SyntaxExpr
{
public:
    ConstSyntaxExpr(const std::string &valueStr = "", 
                    ConstExprType valueType = UNKNOWN,
                    SyntaxExprType type = SYNTAX_EXPR_TYPE_CONST_VALUE,
                    bool _needEncode= false,
                    autil::mem_pool::Pool* pool = NULL);
    
    ~ConstSyntaxExpr();

public:
    /* override */ bool operator == (const SyntaxExpr *expr) const;
    /* override */ void accept(SyntaxExprVisitor *visitor) const;

    /* override */ void serialize(autil::DataBuffer &dataBuffer) const;
    /* override */ void deserialize(autil::DataBuffer &dataBuffer);
    
public:
    std::string getValueString() const { return _valueStr; }
    ConstExprType getValueType() const { return _valueType; }
    bool getNeedEncode() const { return _needEncode; }
private:
    ConstSyntaxExpr(const ConstSyntaxExpr &);
    ConstSyntaxExpr& operator=(const ConstSyntaxExpr &);
private:
    std::string _valueStr;
    ConstExprType _valueType;
    bool _needEncode;
private:
    AUTIL_LOG_DECLARE();
};

}

#endif //ISEARCH_EXPRESSION_CONSTSYNTAXEXPR_H
