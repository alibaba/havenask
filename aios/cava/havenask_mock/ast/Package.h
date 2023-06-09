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

namespace cava {
class Name;

class Package : public ASTNode
{
public:
    Package(Name *name) : _name(name) {}
    ~Package() {}
private:
    Package(const Package &);
    Package& operator=(const Package &);
public:
    Name *getName() { return _name; }
private:
    Name *_name; // maybe null
};

} // end namespace cava

