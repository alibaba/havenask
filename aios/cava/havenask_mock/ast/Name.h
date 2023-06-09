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
#include <vector>

#include "cava/ast/ASTNode.h"
#include "cava/common/Common.h"


namespace cava {

class Name : public ASTNode
{
public:
    ~Name() {}
private:
    Name(const Name &);
    Name& operator=(const Name &);
public:
    std::string &getPostName() { return *_postName; }
    Name *getPrefix() { return _prefix; }
    std::vector<std::string *> &getNameVec() { return _nameVec; }
    uint size() { return _nameVec.size(); }
    Name *getName(uint idx) {
        return nullptr;
    }
private:
    Name *_prefix; // maybe null
    std::string *_postName;
    std::vector<std::string *> _nameVec;
};

} // end namespace cava
