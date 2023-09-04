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
#include <cstdint>
#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "autil/legacy/jsonizable.h"

namespace sql {

// TODO (kong.cui) add enum to string to easy debugging
enum UNNESTINFO_ERRNO {
    UE_OK = 0,
    UE_FIELD_FIELDTYPE_NOT_MATCH = 1,
    UE_FIELD_FIELDCOUNT_NOT_MATCH = 2,
    UE_FIELD_COUNT_INVALID = 3
};

std::string unnestInfoErrno2String(const UNNESTINFO_ERRNO &uErrno) {
    switch (uErrno) {
    case UE_OK:
        return "ok";
    case UE_FIELD_FIELDTYPE_NOT_MATCH:
        return "output field vs output field type size not match";
    case UE_FIELD_FIELDCOUNT_NOT_MATCH:
        return "output filed vs output filed count size not match";
    case UE_FIELD_COUNT_INVALID:
        return "output field count size invalid";
    default:
        return "unknown errno";
    }
}

class CorrelateInfo : public autil::legacy::Jsonizable {
public:
    CorrelateInfo() {}
    ~CorrelateInfo() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("output_fields", _outputFields, _outputFields);
        json.Jsonize("nest_field_names", _nestFieldNames, _nestFieldNames);
        json.Jsonize("with_ordinality", _withOrdinality, false);
        json.Jsonize("output_field_exprs", _outputFieldExprs, _outputFieldExprs);
        json.Jsonize("output_fields_type", _outputFieldsType, _outputFieldsType);
        json.Jsonize("table_name", _tableName, std::string());
        json.Jsonize("nest_field_counts", _nestFieldCounts, _nestFieldCounts);
    }

    UNNESTINFO_ERRNO check() {
        if (_outputFields.size() != _outputFieldsType.size()) {
            return UE_FIELD_FIELDTYPE_NOT_MATCH;
        }
        if (_nestFieldNames.size() != _nestFieldCounts.size()) {
            return UE_FIELD_FIELDCOUNT_NOT_MATCH;
        }

        int32_t total = 0;
        for (int32_t &count : _nestFieldCounts) {
            total += count;
        }
        if (_withOrdinality) {
            total += 1;
        }
        if (static_cast<int32_t>(total) != _outputFields.size()) {
            return UE_FIELD_COUNT_INVALID;
        }

        return UE_OK;
    }

    std::string toString() {
        std::stringstream ss;
        ss << "output_field: [";
        for (auto &field : _outputFields) {
            ss << field << ", ";
        }
        ss << "];;;";
        ss << "output_field_exprs: [";
        for (auto &expr : _outputFieldExprs) {
            ss << expr.first << ": " << expr.second << ", ";
        }
        ss << "];;;";

        return ss.str();
    }

public:
    std::vector<std::string> _outputFields;
    std::vector<std::string> _nestFieldNames;
    std::vector<std::string> _outputFieldsType;
    std::vector<int32_t> _nestFieldCounts;
    std::map<std::string, std::string> _outputFieldExprs;
    std::string _tableName;
    bool _withOrdinality;
};

} // namespace sql
