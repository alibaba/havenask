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

#include <stdint.h>
#include <string>

#include "cava/ast/ASTNode.h"


namespace cava {

class Modifier : public ASTNode
{
public:
    enum Flag {
        F_NONE = 0,
        F_PRIVATE = 1
    };
public:
    Modifier() {}
    ~Modifier() {}
private:
    Modifier(const Modifier &);
    Modifier& operator=(const Modifier &);

public:
    void setFlag(Flag flag) {}
    bool isStatic() { return false;}

    bool isExtern() { return false;}
    bool isExternC() { return false;}
    bool isNative() { return false;}

    bool isPrivate() { return false;}
    bool isPublic() { return false; }
};

} // end namespace cava
