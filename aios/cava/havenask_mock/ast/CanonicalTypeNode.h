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

#include <iosfwd>
#include <string>

#include "cava/ast/TypeNode.h"


namespace cava {

class CanonicalTypeNode : public TypeNode
{
public:
    enum CanonicalType {
        CT_BOOLEAN = 1,
        CT_VOID = 2,
        CT_INT = 3,
        CT_DOUBLE = 4
    };
    
public:
    CanonicalTypeNode(CanonicalType canonicalType)
        : TypeNode(TNK_None)
        , _canonicalType(canonicalType)
    {
    }
    ~CanonicalTypeNode() {}

    std::size_t hash(){
        return 0;
    }
    bool equalTo(TypeNode *other) {
        return false;
    }
    bool convertible(TypeNode *dst) {
        return false;
    }
    std::string toString() {
        return "";
    }
    static bool getCanonicalType(const std::string &typeName, CanonicalType &canonicalType) {
        return false;
    }
private:
    CanonicalTypeNode(const CanonicalTypeNode &);
    CanonicalTypeNode& operator=(const CanonicalTypeNode &);
private:
    CanonicalType _canonicalType;
};

} // end namespace cava
