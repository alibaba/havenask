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

#include "cava/ast/ASTNode.h"
#include "cava/ast/Expr.h"
#include "cava/ast/TypeNode.h" //IWYU pragma: keep
#include "cava/common/Common.h"

namespace cava {

class CGMethodInfo;
class ExprVisitor;

class CallExpr : public Expr
{
public:
    CallExpr(Expr *expr,
             std::string *func,
             std::vector<Expr *> *args)
        : _expr(expr)
    {
    }
    ~CallExpr() {}
private:
    CallExpr(const CallExpr &);
    CallExpr& operator=(const CallExpr &);
public:
    std::string toString() override {
        return "";
    }
    void accept(ExprVisitor *vistor) override {

    }
public:
    Expr *getExpr() { return _expr; }
    std::string &getFuncName() { return *_funcName; }
    std::vector<Expr*> &getArgs() { return *_args; }
    void init(Expr *expr, std::string *func, std::vector<Expr *> *args) {
    }
private:
    Expr *_expr;
    std::string *_funcName;
    std::vector<Expr *> *_args;
};

} // end namespace cava
