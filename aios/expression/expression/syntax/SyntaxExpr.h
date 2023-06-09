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
#ifndef ISEARCH_EXPRESSION_SYNTAXEXPR_H
#define ISEARCH_EXPRESSION_SYNTAXEXPR_H

#include "expression/common.h"
#include "autil/DataBuffer.h"
#include "autil/ConstString.h"
namespace expression {

enum SyntaxExprType {
    SYNTAX_EXPR_TYPE_UNKNOWN,
    SYNTAX_EXPR_TYPE_CONST_VALUE,
    SYNTAX_EXPR_TYPE_ATOMIC_ATTR,
    SYNTAX_EXPR_TYPE_MINUS,
    SYNTAX_EXPR_TYPE_ADD,
    SYNTAX_EXPR_TYPE_DIV,
    SYNTAX_EXPR_TYPE_MUL,
    SYNTAX_EXPR_TYPE_EQUAL,
    SYNTAX_EXPR_TYPE_NOTEQUAL,
    SYNTAX_EXPR_TYPE_LESS,
    SYNTAX_EXPR_TYPE_GREATERTHAN,
    SYNTAX_EXPR_TYPE_LESSEQUAL,
    SYNTAX_EXPR_TYPE_GREATEREQUAL,
    SYNTAX_EXPR_TYPE_AND,
    SYNTAX_EXPR_TYPE_OR,
    SYNTAX_EXPR_TYPE_BITAND,
    SYNTAX_EXPR_TYPE_BITOR,
    SYNTAX_EXPR_TYPE_BITXOR,
    SYNTAX_EXPR_TYPE_FUNC,
    SYNTAX_EXPR_TYPE_FUNC_ARGUMENT,
};

class SyntaxExprVisitor;

class SyntaxExpr
{
public:
    // param: pool is not NULL when deserialize by pool
    SyntaxExpr(SyntaxExprType type = SYNTAX_EXPR_TYPE_UNKNOWN,
               autil::mem_pool::Pool* pool = NULL)
        : _syntaxType(type)
        , _pool(pool)
    {}
    
    virtual ~SyntaxExpr() {}
public:
    virtual bool operator == (const SyntaxExpr *expr) const { return true; }
    virtual void accept(SyntaxExprVisitor *visitor) const {}
    
    const std::string& getExprString() const {
        return _exprString;
    }
    SyntaxExprType getSyntaxType() const {
        return _syntaxType;
    }    
    void setSyntaxType(SyntaxExprType syntaxType) {
        _syntaxType = syntaxType;
    }
public:
    virtual void serialize(autil::DataBuffer &dataBuffer) const = 0;
    virtual void deserialize(autil::DataBuffer &dataBuffer) = 0;
    
public:
    static SyntaxExpr *createSyntax(SyntaxExprType type);
    static SyntaxExpr *createSyntax(SyntaxExprType type, autil::mem_pool::Pool* pool);

    autil::mem_pool::Pool* getPool() const { return _pool; }
    
protected:
    void setExprString(const std::string &exprStr) { _exprString = exprStr; }
    
    void setExprString(const autil::StringView &exprStr)
    { _exprString.assign(exprStr.data(), exprStr.size()); }
    
protected:
    SyntaxExprType _syntaxType;
    std::string _exprString;
    autil::mem_pool::Pool *_pool;
private:
    AUTIL_LOG_DECLARE();
};
EXPRESSION_TYPEDEF_PTR(SyntaxExpr);

}
namespace autil {
template <>
inline void DataBuffer::write<expression::SyntaxExpr>(
        expression::SyntaxExpr const * const &syntax)
{
    bool isExist = syntax;
    write(isExist);
    if (isExist) {
        write(syntax->getSyntaxType());
        write(*syntax);
    }
}

template <>
inline void DataBuffer::read<expression::SyntaxExpr>(expression::SyntaxExpr *&syntax) {
    bool isExist;
    read(isExist);
    if (isExist) {
        expression::SyntaxExprType type;
        read(type);
        syntax = expression::SyntaxExpr::createSyntax(type);
        read(*syntax);
    } else {
        syntax = NULL;
    }
}

template <>
inline void DataBuffer::read<expression::SyntaxExpr>(
        expression::SyntaxExpr *&syntax, autil::mem_pool::Pool *pool) {
    bool isExist;
    read(isExist);
    if (isExist) {
        expression::SyntaxExprType type;
        read(type);
        syntax = expression::SyntaxExpr::createSyntax(type, pool);
        read(*syntax);
    } else {
        syntax = NULL;
    }
}

}

#endif //ISEARCH_EXPRESSION_SYNTAXEXPR_H

