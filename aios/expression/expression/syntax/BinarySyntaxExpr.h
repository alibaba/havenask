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
#ifndef ISEARCH_EXPRESSION_BINARYSYNATXEXPR_H
#define ISEARCH_EXPRESSION_BINARYSYNATXEXPR_H

#include "expression/common.h"
#include "expression/syntax/SyntaxExpr.h"
#include "expression/syntax/SyntaxExprVisitor.h"
namespace expression {

class BinarySyntaxExpr : public SyntaxExpr
{
public:
    BinarySyntaxExpr(SyntaxExpr *a, SyntaxExpr * b, 
                     SyntaxExprType syntaxExprType,
                     autil::mem_pool::Pool* pool = NULL)
        : SyntaxExpr(syntaxExprType, pool)
    {
        _exprA = a;
        _exprB = b;
        if (_exprA && _exprB) {
            setExprString(_exprA, _exprB, syntaxExprType);
        }
    }
    
    virtual ~BinarySyntaxExpr() {
        POOL_COMPATIBLE_DELETE_CLASS(_pool, _exprA);
        POOL_COMPATIBLE_DELETE_CLASS(_pool, _exprB);
    }

public:
    /* override */ bool operator == (const SyntaxExpr *expr) const;

    const SyntaxExpr *getLeftExpr() const { return _exprA; }
    const SyntaxExpr *getRightExpr() const { return _exprB; }
public:
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
private:
    void setExprString(SyntaxExpr *a, SyntaxExpr *b, 
                       SyntaxExprType syntaxExprType);
private:
    SyntaxExpr *_exprA;
    SyntaxExpr *_exprB;

private:
    AUTIL_LOG_DECLARE();
};

class AddSyntaxExpr : public BinarySyntaxExpr
{
public:
    AddSyntaxExpr(SyntaxExpr *a = NULL, SyntaxExpr *b = NULL,
                  autil::mem_pool::Pool* pool = NULL)
        : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_ADD, pool) {
    }
    
    /* override */ void accept(SyntaxExprVisitor *visitor) const { 
        visitor->visitAddSyntaxExpr(this); 
    }
private:
    AUTIL_LOG_DECLARE();
};

class MinusSyntaxExpr : public BinarySyntaxExpr
{
public:
    MinusSyntaxExpr(SyntaxExpr *a = NULL, SyntaxExpr *b = NULL,
                    autil::mem_pool::Pool* pool = NULL)
        : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_MINUS, pool) {
    }
    /* override */ void accept(SyntaxExprVisitor *visitor) const { 
        visitor->visitMinusSyntaxExpr(this);
    }
private:
    AUTIL_LOG_DECLARE();
};

class MulSyntaxExpr : public BinarySyntaxExpr
{
public:
    MulSyntaxExpr(SyntaxExpr *a = NULL, SyntaxExpr *b = NULL,
                  autil::mem_pool::Pool* pool = NULL)
        : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_MUL, pool) {
    }
    /* override */ void accept(SyntaxExprVisitor *visitor) const {
        visitor->visitMulSyntaxExpr(this);
    }
private:
    AUTIL_LOG_DECLARE();
};

class DivSyntaxExpr : public BinarySyntaxExpr
{
public:
    DivSyntaxExpr(SyntaxExpr *a = NULL, SyntaxExpr *b = NULL,
                  autil::mem_pool::Pool* pool = NULL)
        : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_DIV, pool) {
    }
    
    /* override */ void accept(SyntaxExprVisitor *visitor) const {
        visitor->visitDivSyntaxExpr(this);
    }
private:
    AUTIL_LOG_DECLARE();
};

class EqualSyntaxExpr : public BinarySyntaxExpr
{
public:
    EqualSyntaxExpr(SyntaxExpr *a = NULL, SyntaxExpr *b = NULL,
                    autil::mem_pool::Pool* pool = NULL)
        : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_EQUAL, pool) {
    }
    /* override */ void accept(SyntaxExprVisitor *visitor) const {
        visitor->visitEqualSyntaxExpr(this);
    }
private:
    AUTIL_LOG_DECLARE();
};

class NotEqualSyntaxExpr : public BinarySyntaxExpr
{
public:
    NotEqualSyntaxExpr(SyntaxExpr *a = NULL, SyntaxExpr *b = NULL,
                       autil::mem_pool::Pool* pool = NULL)
        : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_NOTEQUAL, pool)
    {
    }
    /* override */ void accept(SyntaxExprVisitor *visitor) const {
        visitor->visitNotEqualSyntaxExpr(this);
    }
private:
    AUTIL_LOG_DECLARE();
};

class LessSyntaxExpr : public BinarySyntaxExpr
{
public:
    LessSyntaxExpr(SyntaxExpr *a = NULL, SyntaxExpr *b = NULL,
                   autil::mem_pool::Pool* pool = NULL)
        : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_LESS, pool) {
    }
    /* override */ void accept(SyntaxExprVisitor *visitor) const {
        visitor->visitLessSyntaxExpr(this);
    }
private:
    AUTIL_LOG_DECLARE();
};

class GreaterSyntaxExpr : public BinarySyntaxExpr
{
public:
    GreaterSyntaxExpr(SyntaxExpr *a = NULL, SyntaxExpr *b = NULL,
                      autil::mem_pool::Pool* pool = NULL)
        : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_GREATERTHAN, pool) {
    }
    /* override */ void accept(SyntaxExprVisitor *visitor) const {
        visitor->visitGreaterSyntaxExpr(this);
    }
private:
    AUTIL_LOG_DECLARE();
};

class LessEqualSyntaxExpr : public BinarySyntaxExpr
{
public:
    LessEqualSyntaxExpr(SyntaxExpr *a = NULL, SyntaxExpr *b = NULL,
                        autil::mem_pool::Pool* pool = NULL)
        : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_LESSEQUAL, pool) {
    }
    /* override */ void accept(SyntaxExprVisitor *visitor) const {
        visitor->visitLessEqualSyntaxExpr(this);
    }
private:
    AUTIL_LOG_DECLARE();
};

class GreaterEqualSyntaxExpr : public BinarySyntaxExpr
{
public:
    GreaterEqualSyntaxExpr(SyntaxExpr *a = NULL, SyntaxExpr *b = NULL,
                           autil::mem_pool::Pool* pool = NULL)
        : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_GREATEREQUAL, pool) {
    }
    /* override */ void accept(SyntaxExprVisitor *visitor) const {
        visitor->visitGreaterEqualSyntaxExpr(this);
    }
private:
    AUTIL_LOG_DECLARE();
};

class AndSyntaxExpr : public BinarySyntaxExpr
{
public:
    AndSyntaxExpr(SyntaxExpr *a = NULL, SyntaxExpr *b = NULL,
                  autil::mem_pool::Pool* pool = NULL)
        : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_AND, pool) {
    }
    /* override */ void accept(SyntaxExprVisitor *visitor) const {
        visitor->visitAndSyntaxExpr(this);
    }
private:
    AUTIL_LOG_DECLARE();
};

class OrSyntaxExpr : public BinarySyntaxExpr
{
public:
    OrSyntaxExpr(SyntaxExpr *a = NULL, SyntaxExpr *b = NULL,
                 autil::mem_pool::Pool* pool = NULL)
        : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_OR, pool) {
    }
    /* override */ void accept(SyntaxExprVisitor *visitor) const {
        visitor->visitOrSyntaxExpr(this);
    }
private:
    AUTIL_LOG_DECLARE();
};

class BitOrSyntaxExpr : public BinarySyntaxExpr
{
public:
    BitOrSyntaxExpr(SyntaxExpr *a = NULL, SyntaxExpr *b = NULL,
                    autil::mem_pool::Pool* pool = NULL)
        : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_BITOR, pool) {
    }
    /* override */ void accept(SyntaxExprVisitor *visitor) const {
        visitor->visitBitOrSyntaxExpr(this);
    }
private:
    AUTIL_LOG_DECLARE();
};

class BitAndSyntaxExpr : public BinarySyntaxExpr
{
public:
    BitAndSyntaxExpr(SyntaxExpr *a = NULL, SyntaxExpr *b = NULL,
                     autil::mem_pool::Pool* pool = NULL)
        : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_BITAND, pool) {
    }
    /* override */ void accept(SyntaxExprVisitor *visitor) const {
        visitor->visitBitAndSyntaxExpr(this);
    }
private:
    AUTIL_LOG_DECLARE();
};

class BitXorSyntaxExpr : public BinarySyntaxExpr
{
public:
    BitXorSyntaxExpr(SyntaxExpr *a = NULL, SyntaxExpr *b = NULL,
                     autil::mem_pool::Pool* pool = NULL)
        : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_BITXOR, pool) {
    }
    /* override */ void accept(SyntaxExprVisitor *visitor) const {
        visitor->visitBitXorSyntaxExpr(this);
    }
private:
    AUTIL_LOG_DECLARE();
};

}

#endif //ISEARCH_EXPRESSION_BINARYSYNATXEXPR_H
