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

#include "autil/legacy/jsonizable.h"

namespace isearch {
namespace sql {

class FieldMeta : public autil::legacy::Jsonizable {
public:
    FieldMeta() {}
    ~FieldMeta() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("field_name", fieldName);
        json.Jsonize("field_type", fieldType);
        json.Jsonize("index_name", indexName, "");
        json.Jsonize("index_type", indexType, "");
    }

public:
    std::string fieldName;
    std::string fieldType;
    std::string indexName;
    std::string indexType;
};

} // namespace sql
} // namespace isearch
