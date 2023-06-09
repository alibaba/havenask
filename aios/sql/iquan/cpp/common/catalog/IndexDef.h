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
#include <vector>

#include "autil/legacy/jsonizable.h"

namespace iquan {

class IndexDef : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("index_name", indexName);
        json.Jsonize("index_type", indexType);
        json.Jsonize("index_fields", indexFields);
        json.Jsonize("value_fields", valueFields, valueFields);
    }

public:
    std::string indexName;
    std::string indexType;
    std::vector<std::string> indexFields;
    std::vector<std::string> valueFields;
};

} // namespace iquan
