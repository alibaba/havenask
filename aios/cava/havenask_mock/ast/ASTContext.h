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
#include "cava/common/Common.h"

namespace cava {

class ClassDecl;
class TypeNode;
class Expr;
class Formal;

class ASTContext
{
public:
    enum Status {
        S_SYNTAX_ERROR
    } ;
public:
    ~ASTContext() {
        
    }
private:
    ASTContext(const ASTContext &);
    ASTContext& operator=(const ASTContext &);
public:
    std::string *allocString(std::string str) {
        return nullptr;
    }
    std::vector<Expr *>* allocExprVec() {
        return nullptr;
    }
    std::vector<Formal*>* allocFormalVec() {
        return nullptr;
    }
    std::vector<Stmt*>* allocStmtVec() {
        return nullptr;
    }
    Stmt* createExprStmt() {
        return nullptr;
    }
    Stmt* createReturnStmt() {
        return nullptr;
    }
    std::vector<ClassDecl*>& getClassDecls() {
        return classDecs;
    }
private:
    std::vector<ClassDecl*> classDecs;
    std::vector<std::string *> _strings;
};


} // end namespace cava
