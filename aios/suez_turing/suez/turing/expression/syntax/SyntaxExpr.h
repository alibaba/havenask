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

#include <memory>
#include <stddef.h>
#include <string>

#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "matchdoc/ValueType.h"
#include "suez/turing/expression/common.h"

namespace suez {
namespace turing {
class SyntaxExprVisitor;

// the result type of 'syntax' & 'attribute' expression
typedef matchdoc::BuiltinType ExprResultType;

enum SyntaxExprType {
    SYNTAX_EXPR_TYPE_UNKNOWN,
    SYNTAX_EXPR_TYPE_CONST_VALUE,
    SYNTAX_EXPR_TYPE_ATOMIC_ATTR,
    SYNTAX_EXPR_TYPE_VIRTUAL_ATTR,
    SYNTAX_EXPR_TYPE_MINUS,
    SYNTAX_EXPR_TYPE_ADD,
    SYNTAX_EXPR_TYPE_DIV,
    SYNTAX_EXPR_TYPE_MUL,
    SYNTAX_EXPR_TYPE_POWER,
    SYNTAX_EXPR_TYPE_LOG,
    SYNTAX_EXPR_TYPE_EQUAL,
    SYNTAX_EXPR_TYPE_NOTEQUAL,
    SYNTAX_EXPR_TYPE_LESS,
    SYNTAX_EXPR_TYPE_GREATERTHAN,
    SYNTAX_EXPR_TYPE_LESSEQUAL,
    SYNTAX_EXPR_TYPE_GREATEREQUAL,
    SYNTAX_EXPR_TYPE_AND,
    SYNTAX_EXPR_TYPE_OR,
    SYNTAX_EXPR_TYPE_RANK,
    SYNTAX_EXPR_TYPE_BITAND,
    SYNTAX_EXPR_TYPE_BITOR,
    SYNTAX_EXPR_TYPE_BITXOR,
    SYNTAX_EXPR_TYPE_CONDITIONAL,
    SYNTAX_EXPR_TYPE_FUNC,
    SYNTAX_EXPR_TYPE_FUNC_ARGUMENT,
    SYNTAX_EXPR_TYPE_MULTI,
};

class SyntaxExpr {
public:
    SyntaxExpr(ExprResultType resultType = vt_unknown, SyntaxExprType syntaxType = SYNTAX_EXPR_TYPE_UNKNOWN) {
        _isMultiValue = false;
        _isSubExpression = false;
        _syntaxType = syntaxType;
        _resultType = resultType;
    }
    virtual ~SyntaxExpr(){};

public:
    virtual bool operator==(const SyntaxExpr *expr) const = 0;
    virtual void accept(SyntaxExprVisitor *visitor) const = 0;

    void setExprResultType(ExprResultType resultType) const { _resultType = resultType; }
    ExprResultType getExprResultType() const { return _resultType; }

    void setSyntaxExprType(SyntaxExprType syntaxType) const { _syntaxType = syntaxType; }
    SyntaxExprType getSyntaxExprType() const { return _syntaxType; }

    void setExprString(const std::string &exprString) { _exprString = exprString; }
    virtual std::string getExprString() const { return _exprString; }

    bool isMultiValue() const { return _isMultiValue; }
    void setMultiValue(bool isMultiValue) const { _isMultiValue = isMultiValue; }

    void setIsSubExpression(bool flag) const { _isSubExpression = flag; }
    bool isSubExpression() const { return _isSubExpression; }

    virtual void serialize(autil::DataBuffer &dataBuffer) const;
    virtual void deserialize(autil::DataBuffer &dataBuffer);
    static SyntaxExpr *createSyntaxExpr(SyntaxExprType type);

protected:
    mutable bool _isMultiValue;
    mutable bool _isSubExpression;
    mutable SyntaxExprType _syntaxType;
    mutable ExprResultType _resultType;
    std::string _exprString;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SyntaxExpr> SyntaxExprPtr;

} // namespace turing
} // namespace suez

namespace autil {
template <>
inline void DataBuffer::write<suez::turing::SyntaxExpr>(suez::turing::SyntaxExpr const *const &p) {
    bool isNull = p;
    write(isNull);
    if (isNull) {
        suez::turing::SyntaxExprType type = p->getSyntaxExprType();
        write(type);
        write(*p);
    }
}

template <>
inline void DataBuffer::read<suez::turing::SyntaxExpr>(suez::turing::SyntaxExpr *&p) {
    bool isNull;
    read(isNull);
    if (isNull) {
        suez::turing::SyntaxExprType type;
        read(type);
        p = suez::turing::SyntaxExpr::createSyntaxExpr(type);
        read(*p);
    } else {
        p = NULL;
    }
}
} // namespace autil
