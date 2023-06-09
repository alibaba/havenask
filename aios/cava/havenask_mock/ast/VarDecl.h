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

#include "cava/ast/ASTNode.h"
#include "cava/ast/Expr.h" //IWYU pragma: keep
#include "cava/common/Common.h"

namespace cava {

class CGClassInfo;

class VarDecl : public ASTNode
{
public:
    VarDecl(std::string *name)
    {
    }
    ~VarDecl() {}
private:
    VarDecl(const VarDecl &);
    VarDecl& operator=(const VarDecl &);
public:
    std::string toString() override {
        return "";
    }
public:
    std::string &getVarName() { return *_varName; }
    Expr *getInitExpr() { return nullptr; }
private:
    std::string *_varName;
};

typedef VarDecl* DeclId;

} // end namespace cava
