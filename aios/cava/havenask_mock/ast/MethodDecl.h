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
#include "cava/ast/Stmt.h"
#include "cava/common/Common.h"


namespace cava {
class Formal;
class Modifier;
class TypeNode;

class MethodDecl : public ASTNode
{
public:
    virtual ~MethodDecl() {}
private:
    MethodDecl(const MethodDecl &);
    MethodDecl& operator=(const MethodDecl &);
public:
    std::string toString() override {
        return "";
    }
    Modifier *getModifier() { return _modifier; }
    std::string &getMethodName() { return *_methodName; }
    std::vector<Formal *> &getFormals() {
        return *_formals;
    }
    TypeNode *getRetType() { return nullptr; }
    void* getBody() {
        return nullptr;
    }
    void setBody(Stmt* body) {
    }
private:
    Modifier *_modifier;
    std::string *_methodName;
    std::vector<Formal *>* _formals;
};

} // end namespace cava
