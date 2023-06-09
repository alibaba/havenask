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

#include "cava/ast/Stmt.h"
#include "cava/ast/VarDecl.h"

namespace cava {
class Expr;
class TypeNode;

class LocalVarDeclStmt : public Stmt
{
public:
    LocalVarDeclStmt(TypeNode *typeNode,
                     VarDecl *varDecl)
        : Stmt(SK_NONE)
    {
    }
    ~LocalVarDeclStmt() {}
private:
    LocalVarDeclStmt(const LocalVarDeclStmt &);
    LocalVarDeclStmt& operator=(const LocalVarDeclStmt &);
public:
    std::string toString() override;
public:
    TypeNode *getTypeNode() { return nullptr; }
    const std::string &getVarName() { return _varDecl->getVarName(); }
    DeclId getDeclId() { return _varDecl; }
    Expr *getInitExpr() { return nullptr; } // maybe NULL
private:
    TypeNode *_typeNode;
    VarDecl *_varDecl;
};

} // end namespace cava
