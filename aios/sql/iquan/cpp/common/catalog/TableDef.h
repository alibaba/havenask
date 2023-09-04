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

#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "iquan/common/catalog/IndexDef.h"
#include "iquan/common/catalog/LocationDef.h"

namespace iquan {

class FieldTypeDef : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        if (json.GetMode() == TO_JSON) {
            json.Jsonize("type", typeName);
            json.Jsonize("extend_infos", extendInfos);
            if (keyType.size() > 0) {
                json.Jsonize("key_type", keyType);
            }

            if (valueType.size() > 0) {
                json.Jsonize("value_type", valueType);
            }
        } else {
            json.Jsonize("type", typeName, std::string());
            json.Jsonize("extend_infos", extendInfos, extendInfos);
            json.Jsonize("key_type", keyType, keyType);
            json.Jsonize("value_type", valueType, valueType);
        }
    }

public:
    std::string typeName;
    std::map<std::string, std::string> extendInfos;
    autil::legacy::json::JsonMap keyType;
    autil::legacy::json::JsonMap valueType;
};

class FieldDef : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("field_name", fieldName);
        if (json.GetMode() == TO_JSON) {
            if (!indexType.empty() && !indexName.empty()) {
                json.Jsonize("index_type", indexType);
                json.Jsonize("index_name", indexName);
            }
            json.Jsonize("field_type", fieldType);
            json.Jsonize("is_attribute", isAttr);
        } else {
            json.Jsonize("index_type", indexType, indexType);
            json.Jsonize("index_name", indexName, indexName);
            json.Jsonize("field_type", fieldType, fieldType);
            json.Jsonize("is_attribute", isAttr, isAttr);
        }
    }

    bool isValid() const {
        return (indexType.empty() && indexName.empty())
               || (!indexType.empty() && !indexName.empty());
    }

public:
    std::string fieldName;
    FieldTypeDef fieldType;
    std::string indexType = "";
    std::string indexName = "";
    bool isAttr = false;
};

class DistributionDef : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("partition_cnt", partitionCnt);
        json.Jsonize("hash_fields", hashFields);
        json.Jsonize("hash_function", hashFunction);
        json.Jsonize("hash_params", hashParams, hashParams);
    }

    bool isValid() const {
        return true;
    }

public:
    int partitionCnt = 0;
    std::vector<std::string> hashFields;
    std::string hashFunction;
    std::map<std::string, std::string> hashParams;
};

class SortDescDef : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("field", field);
        json.Jsonize("order", order);
    }

    bool isValid() const {
        return true;
    }

public:
    std::string field;
    std::string order;
};

class JoinInfoDef : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        if (json.GetMode() == TO_JSON) {
            if (!tableName.empty() && !joinField.empty()) {
                json.Jsonize("table_name", tableName);
                json.Jsonize("join_field", joinField);
            }
        } else {
            json.Jsonize("table_name", tableName, tableName);
            json.Jsonize("join_field", joinField, joinField);
        }
    }

    bool isValid() const {
        return (tableName.empty() && joinField.empty())
               || (!tableName.empty() && !joinField.empty());
    }

public:
    std::string tableName = "";
    std::string joinField = "";
};

class SubTableDef : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("sub_table_name", subTableName);
        json.Jsonize("sub_fields", subFields);
    }

    bool isValid() const {
        return true;
    }

public:
    std::string subTableName;
    std::vector<FieldDef> subFields;
};

class SchemaDef : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("fields", fields);
        json.Jsonize("summary_fields", summaryFields);
        json.Jsonize("sub_tables", subTables, subTables);
    }

public:
    std::vector<FieldDef> fields;
    std::vector<FieldDef> summaryFields;
    std::vector<SubTableDef> subTables;
};

class TableDef : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("table_name", tableName);
        json.Jsonize("table_type", tableType);
        json.Jsonize("fields", fields);
        json.Jsonize("summary_fields", summaryFields, summaryFields);
        json.Jsonize("sub_tables", subTables, subTables);
        json.Jsonize("distribution", distribution);
        json.Jsonize("sort_desc", sortDesc, sortDesc);
        json.Jsonize("location", location, location);
        json.Jsonize("properties", properties);
        json.Jsonize("indexes", indexes, indexes);
        jsonJoinInfo(json);
    }

    bool isValid() const {
        return true;
    }

private:
    void jsonJoinInfo(autil::legacy::Jsonizable::JsonWrapper &json) {
        if (json.GetMode() == FROM_JSON) {
            // compatible to old config format
            try {
                JoinInfoDef def;
                json.Jsonize("join_info", def);
                if (!def.tableName.empty() && !def.joinField.empty()) {
                    joinInfo.push_back(def);
                }
                return;
            } catch (...) {}
        }
        json.Jsonize("join_info", joinInfo, joinInfo);
    }

public:
    std::string tableName;
    std::string tableType;
    std::vector<FieldDef> fields;
    std::vector<FieldDef> summaryFields;
    std::vector<SubTableDef> subTables;
    DistributionDef distribution;
    LocationDef location;
    std::vector<SortDescDef> sortDesc;
    std::vector<JoinInfoDef> joinInfo;
    std::map<std::string, std::string> properties;
    std::map<std::string, std::vector<IndexDef>> indexes;
};

constexpr char PROP_KEY_IS_SUMMARY[] = "is_summary";

} // namespace iquan
