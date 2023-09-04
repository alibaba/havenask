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
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "iquan/common/catalog/FunctionCommonDef.h"

namespace iquan {

class TvfFieldDef : public autil::legacy::Jsonizable {
public:
    TvfFieldDef() = default;
    TvfFieldDef(const std::string &fieldName, const ParamTypeDef &fieldType)
        : fieldName(fieldName)
        , fieldType(fieldType) {}

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("field_name", fieldName, fieldName);
        json.Jsonize("field_type", fieldType, fieldType);
    }

public:
    std::string fieldName;
    ParamTypeDef fieldType;
};

class TvfInputTableDef : public autil::legacy::Jsonizable {
public:
    TvfInputTableDef()
        : autoInfer(false) {}

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("auto_infer", autoInfer, autoInfer);
        json.Jsonize("input_fields", inputFields, inputFields);
        json.Jsonize("check_fields", checkFields, checkFields);
    }

public:
    bool autoInfer;
    std::vector<TvfFieldDef> inputFields;
    std::vector<TvfFieldDef> checkFields;
};

class TvfParamsDef : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("scalars", scalars, scalars);
        json.Jsonize("tables", tables, tables);
    }

public:
    std::vector<ParamTypeDef> scalars;
    std::vector<TvfInputTableDef> tables;
};

class TvfOutputTableDef : public autil::legacy::Jsonizable {
public:
    TvfOutputTableDef()
        : autoInfer(false) {}

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("auto_infer", autoInfer, autoInfer);
        json.Jsonize("field_names", fieldNames, fieldNames);
    }

public:
    bool autoInfer;
    std::vector<std::string> fieldNames;
};

class TvfReturnsDef : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("new_fields", newFields, newFields);
        json.Jsonize("tables", outputTables, outputTables);
    }

public:
    std::vector<TvfFieldDef> newFields;
    std::vector<TvfOutputTableDef> outputTables;
};

class TvfDef : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("params", params, params);
        json.Jsonize("returns", returns, returns);
    }

public:
    TvfParamsDef params;
    TvfReturnsDef returns;
};

class TvfDistributionDef : public autil::legacy::Jsonizable {
public:
    TvfDistributionDef()
        : partitionCnt(0) {}

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("partition_cnt", partitionCnt, partitionCnt);
    }

public:
    int64_t partitionCnt;
};

class TvfFunctionDef : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("prototypes", tvfs, tvfs);
        json.Jsonize("properties", properties, properties);
        json.Jsonize("distribution", distribution, distribution);
    }

public:
    std::vector<TvfDef> tvfs;
    TvfDistributionDef distribution;
    autil::legacy::json::JsonMap properties;
};

} // namespace iquan
