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

class Import : public ASTNode
{
public:
    enum ImportType {
        IT_SINGLE,
        IT_ON_DEMAND
    };
public:
    Import(Name *name, ImportType importType)
        : _name(name)
        , _importType(importType)
    {
    }
    ~Import() {}
private:
    Import(const Import &);
    Import& operator=(const Import &);
public:
    std::string toString() override {
        return "";
    }
    Name *getName() { return nullptr; }
    ImportType getImportType() { return _importType; }
private:
    Name *_name;
    ImportType _importType;
};

} // end namespace cava
