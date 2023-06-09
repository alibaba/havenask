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

#include <cstddef>
#include <functional>
#include <string>
#include <system_error>

#include "cava/ast/ASTNode.h"
#include "cava/common/Common.h"


namespace cava {

class TypeNode : public ASTNode
{
public:
    enum TypeNodeKind {
        TNK_Amb,
        TNK_Array,
        TNK_None,
        TNK_Canonical
    };
public:
    TypeNode(TypeNodeKind kind) {}
    virtual ~TypeNode() {}
private:
    TypeNode(const TypeNode &);
    TypeNode& operator=(const TypeNode &);
public:
    virtual bool convertible(TypeNode *dst) = 0;
    TypeNodeKind getKind() { return _kind; }
protected:
    TypeNodeKind _kind;
};

} // end namespace std

