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

#include <map>
#include <string>

#include "autil/legacy/jsonizable.h"

namespace isearch {
namespace sql {

class IndexInfo : public autil::legacy::Jsonizable {
public:
    IndexInfo() {}
    IndexInfo(std::string name_, std::string type_)
        : name(name_)
        , type(type_) {}
    ~IndexInfo() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("name", name);
        json.Jsonize("type", type);
    }

public:
    std::string name;
    std::string type;
};

std::ostream &operator << (std::ostream &os, const IndexInfo &indexInfo);

typedef std::map<std::string, sql::IndexInfo> IndexInfoMapType;

} // namespace sql
} // namespace isearch
