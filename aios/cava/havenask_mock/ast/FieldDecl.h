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

#include "cava/ast/ASTNode.h"
#include "cava/ast/Modifier.h"
#include "cava/ast/VarDecl.h"

namespace cava {
class TypeNode;

class FieldDecl : public ASTNode
{
public:
    FieldDecl(Modifier *modifier,
              TypeNode *type,
              VarDecl *var) {
    }
    ~FieldDecl() {}
private:
    FieldDecl(const FieldDecl &);
    FieldDecl& operator=(const FieldDecl &);
public:
    std::string toString() override {
        return "";
    }
public:
    Modifier *getModifier() { return nullptr; }
    TypeNode *getTypeNode() { return nullptr; }
    std::string &getFieldName() { return _varDecl->getVarName(); }
    bool isStatic() { return false;}
    DeclId getDeclId() { return _varDecl; }
private:
    Modifier *_modifier;
    TypeNode *_typeNode;
    VarDecl *_varDecl;
};

} // end namespace cava
