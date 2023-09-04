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
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "sql/common/FieldInfo.h"
#include "sql/common/FieldMeta.h"
#include "sql/common/IndexInfo.h"

namespace sql {

class TableMeta : public autil::legacy::Jsonizable {
public:
    TableMeta() {}
    ~TableMeta() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("field_meta", fieldsMeta, fieldsMeta);
    }

    void extractIndexInfos(std::map<std::string, IndexInfo> &indexInfos) const {
        indexInfos.clear();
        for (const auto &fieldMeta : fieldsMeta) {
            if (!fieldMeta.indexName.empty()
                && fieldMeta.fieldName.find('.') == std::string::npos) {
                // skip aux table index
                indexInfos[fieldMeta.fieldName]
                    = IndexInfo(fieldMeta.indexName, fieldMeta.indexType);
            }
        }
    }

    void extractFieldInfos(std::map<std::string, FieldInfo> &fieldInfos) const {
        fieldInfos.clear();
        for (const auto &fieldMeta : fieldsMeta) {
            if (!fieldMeta.fieldName.empty()) {
                fieldInfos[fieldMeta.fieldName]
                    = FieldInfo(fieldMeta.fieldName, fieldMeta.fieldType);
            }
        }
    }

    std::string getPkIndexName() const {
        for (const auto &fieldMeta : fieldsMeta) {
            if (fieldMeta.indexType == "primarykey64" || fieldMeta.indexType == "primarykey128"
                || fieldMeta.indexType == "primary_key") {
                return fieldMeta.indexName;
            }
        }
        return "";
    }

public:
    std::vector<FieldMeta> fieldsMeta;
};

} // namespace sql
