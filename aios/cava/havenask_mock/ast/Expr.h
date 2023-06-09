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

#include "cava/ast/ASTNode.h"

namespace cava {

class ExprVisitor;
class TypeNode;

class Expr : public ASTNode
{
public:
    Expr() {}
    virtual ~Expr() {}
private:
    Expr(const Expr &);
    Expr& operator=(const Expr &);
public:
    virtual void accept(ExprVisitor *vistor) = 0;
public:
    TypeNode *getType();
private:
    
};

} // end namespace cava
