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
#include <vector>

#include "cava/ast/Stmt.h"

namespace cava {
class Expr;

class ForStmt : public Stmt
{
public:
    ForStmt(std::vector<Stmt *> *initStmts,
            Expr *cond,
            std::vector<Expr *> *updateExprs,
            Stmt *body)
        : Stmt(SK_NONE)
    {
    }
    ~ForStmt() {}
private:
    ForStmt(const ForStmt &);
    ForStmt& operator=(const ForStmt &);
public:
    std::string toString() override;
public:
    std::vector<Stmt *> &getInitStmts() { return *_initStmts; }
    Expr *getCond() { return _cond; }
    std::vector<Expr *> &getUpdateExprs() { return *_updateExprs; }
    Stmt *getBody() { return _body; }
    void setBody(Stmt *body) { _body = body; }
private:
    std::vector<Stmt *> *_initStmts;
    Expr *_cond; // maybe NULL
    std::vector<Expr *> *_updateExprs;
    Stmt *_body;
};

} // end namespace cava
