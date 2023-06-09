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
#include "cava/ast/ClassBodyDecl.h"
#include "cava/common/Common.h"


namespace cava {
class ConstructorDecl;
class FieldDecl;
class Import;
class MethodDecl;
class Modifier;
class Package;
class TypeNode;

class ClassDecl : public ASTNode
{
public:
    ~ClassDecl() {}
private:
    ClassDecl(const ClassDecl &);
    ClassDecl& operator=(const ClassDecl &);
public:
    // called after AST parse
    std::string &getClassName() { return *_className; }
    std::vector<MethodDecl *> &getMethods() {
        return *method;
    }
    bool isTemplateClass() { return false;}
    // setPackage getPackage
    DECLARE_SET_GET_PTR_FUNC(Package, Package, _package);
public:
    std::string getFullClassName() {
        return "";
    }
    std::vector<FieldDecl *> &getFields() {
        return *field;
    }
    ClassBodyDecl *getClassBodyDecl() {
        return nullptr;
    }
    void *setImports(std::vector<Import*>* imports) {
        return nullptr;
    }
private:
    std::string *_className;
    std::vector<MethodDecl *> * method;
    std::vector<FieldDecl *> * field;
};

} // end namespace cava
