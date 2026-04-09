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

#include <algorithm>
#include <string>
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "iquan/common/catalog/FieldIdentity.h"
#include "iquan/common/catalog/FunctionModel.h"
#include "iquan/common/catalog/JoinInfoDef.h"
#include "iquan/common/catalog/TableIdentity.h"

namespace iquan {

struct LocationSign {
    uint32_t partitionCnt = 0;
    std::string nodeName;
    std::string nodeType;
    std::string tableName;
};

class LocationDef : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("partition_cnt", sign.partitionCnt, sign.partitionCnt);
        json.Jsonize("node_name", sign.nodeName, sign.nodeName);
        json.Jsonize("location_table_name",sign.tableName,sign.tableName);
        json.Jsonize("node_type", sign.nodeType, sign.nodeType);
        json.Jsonize("tables", tableIdentities, tableIdentities);
        json.Jsonize("equivalent_hash_fields", equilvalentHashFields, equilvalentHashFields);
        json.Jsonize("join_info", joinInfos, joinInfos);
    }

    bool isValid() const {
        for (const auto &tableIdentity : tableIdentities) {
            if (!tableIdentity.isValid()) {
                return false;
            }
        }

        for (const auto &fieldVec : equilvalentHashFields) {
            for (const auto &fieldIdentity : fieldVec) {
                if (!fieldIdentity.isValid()) {
                    return false;
                }
            }
        }

        for (const auto &joinInfo : joinInfos) {
            if (!joinInfo.isValid()) {
                return false;
            }
        }

        return sign.partitionCnt > 0 && !sign.nodeName.empty() && !sign.nodeType.empty();
    }

    bool merge(LocationDef &other) {
        if (other.sign.nodeName != sign.nodeName) {
            return false;
        }
        bool ret = true;
        for (const auto &tableIdentity : other.tableIdentities) {
            ret = ret && addTableIdentity(tableIdentity);
        }
        for (const auto &joinInfoDef : other.joinInfos) {
            ret = ret && addJoinInfo(joinInfoDef);
        }
        for (const auto &hashField : other.equilvalentHashFields) {
            ret = ret && addHashField(hashField);
        }
        for (const auto &func : other.functions) {
            ret = ret && addFunction(func);
        }
        return ret;
    }

    bool addTableIdentity(const TableIdentity &tableId) {
        if (std::find(tableIdentities.begin(), tableIdentities.end(), tableId)
            != tableIdentities.end()) {
            return false;
        }
        tableIdentities.emplace_back(tableId);
        return true;
    }

    bool addJoinInfo(const JoinInfoDef &joinInfoDef) {
        if (std::find(joinInfos.begin(), joinInfos.end(), joinInfoDef) != joinInfos.end()) {
            return false;
        }
        joinInfos.push_back(joinInfoDef);
        return true;
    }

    bool addHashField(const std::vector<FieldIdentity> &hashField) {
        for (const auto &thisHashField : equilvalentHashFields) {
            if (thisHashField.size() == hashField.size()) {
                bool equal = true;
                for (int i = 0; i < thisHashField.size(); ++i) {
                    equal = equal && hashField[i] == thisHashField[i];
                    if (!equal) {
                        break;
                    }
                }
                if (equal) {
                    return false;
                }
            }
        }
        equilvalentHashFields.push_back(hashField);
        return true;
    }

    bool addFunction(const FunctionSign &functionSign) {
        if (hasFunction(functionSign)) {
            return false;
        }
        functions.push_back(functionSign);
        return true;
    }

    bool hasFunction(const FunctionSign &functionSign) const {
        if (std::find(functions.begin(), functions.end(), functionSign) != functions.end()) {
            return true;
        }
        return false;
    }

public:
    LocationSign sign;
    std::vector<TableIdentity> tableIdentities;
    std::vector<std::vector<FieldIdentity>> equilvalentHashFields;
    std::vector<JoinInfoDef> joinInfos;
    std::vector<FunctionSign> functions;
};

} // namespace iquan
