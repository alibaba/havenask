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

#include <string>

#include "cava/ast/Expr.h"


namespace cava {
class ExprVisitor;

class LiteralExpr : public Expr
{
public:
    enum LiteralType {
        LT_NULL,
        LT_INT,
        LT_STRING,
        LT_BOOLEAN
    };
public:
    LiteralExpr(LiteralType type) {}
    virtual ~LiteralExpr() {}
private:
    LiteralExpr(const LiteralExpr &);
    LiteralExpr& operator=(const LiteralExpr &);
public:
    std::string toString() override;
    void accept(ExprVisitor *vistor) override;
    virtual std::string getRawVaule() { return ""; }
public:
    LiteralType getLiteralType() { return _literalType; }
protected:
    LiteralType _literalType;
private:
    
};

template <class T>
class TypedLiteralExpr : public LiteralExpr {
public:
    TypedLiteralExpr(LiteralType type, T v)
        : LiteralExpr(type), val(v) {}
    ~TypedLiteralExpr() {}
public:
    T val;
public:
    std::string toString() {
        return "";
    }
    virtual std::string getRawVaule() { return ""; }
};

class NullLiteralExpr : public LiteralExpr {
public:
    NullLiteralExpr()
        : LiteralExpr(LiteralExpr::LT_NULL) {}
public:
    std::string toString() {
        return "";
    }
    virtual std::string getRawVaule() { return ""; }
};

template <>
class TypedLiteralExpr<std::string *> : public LiteralExpr {
public:
    TypedLiteralExpr(LiteralType type, std::string *v)
        : LiteralExpr(type), val(v) {}
    ~TypedLiteralExpr() {}
public:
    std::string toString() {
        return "";
    }
    virtual std::string getRawVaule() { return ""; }
public:
    std::string *val;
};


} // end namespace cava
