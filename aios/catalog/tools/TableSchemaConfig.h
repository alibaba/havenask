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
#include "autil/result/Result.h"
#include "catalog/entity/Table.h"

namespace catalog {

struct FieldConfig : public autil::legacy::Jsonizable {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    bool init(const proto::TableStructure::Column &column);

    std::string fieldName;
    std::string fieldType;
    bool isMultiValue = false;
    std::string defaultStrValue;
    bool supportNull = false;
    bool updatable = false;
    std::string separator;
    std::string analyzer;
};

struct IndexConfig : public autil::legacy::Jsonizable {
    virtual bool init(const proto::TableStructure::Index &index) = 0;
    std::string convertCompressType(proto::TableStructure::Index::IndexConfig::CompressType compressType);

    std::string indexName;
    std::string indexType;
    std::string fileCompressor;

    static std::vector<std::string> intParams;
    static std::vector<std::string> boolParams;

protected:
    void
    jsonizeOneParam(autil::legacy::Jsonizable::JsonWrapper &json, const std::string &key, const std::string &value);
};

struct AttributeConfig : public IndexConfig {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    bool init(const proto::TableStructure::Index &index) override;

    std::string fieldName;
    bool updatable = false;
    std::map<std::string, std::string> indexParams;
};

struct SummaryConfig : public IndexConfig {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    bool init(const proto::TableStructure::Index &index) override;

    std::vector<std::string> fieldNames;
    std::map<std::string, std::string> parameters;
};

struct PrimaryKeyIndexConfig : IndexConfig {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    bool init(const proto::TableStructure::Index &index) override;

    std::string indexFields;
    std::map<std::string, std::string> indexParams;
};

struct OrcIndexConfig : IndexConfig {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    bool init(const proto::TableStructure::Index &index) override;

    std::vector<std::string> indexFields;
};

struct PackField : public autil::legacy::Jsonizable {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;

    std::string fieldName;
    int32_t boost;
};

struct InvertedIndexConfig : public IndexConfig {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    bool init(const catalog::proto::TableStructure::Index &index) override;
    bool hasMultiFields(const std::string &indexType);

    std::string indexFields;
    std::vector<PackField> packIndexFields;
    std::map<std::string, std::string> indexParams;
    std::map<std::string, std::string> parameters;
};

struct TableSchemaConfig final : autil::legacy::Jsonizable {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;

    bool init(const std::string &name, const TableStructure *tableStructure);
    autil::Result<std::string> ConvertTableType(proto::TableType::Code code);
    static autil::Result<std::string> convertDataType(proto::TableStructure::Column::DataType dataType);
    static autil::Result<std::string> convertIndexType(proto::TableStructure::Index::IndexType indexType);

    bool operator==(const TableSchemaConfig &other) const;

    std::string tableName;
    std::string tableType;
    std::vector<FieldConfig> fields;
    std::map<std::string, std::vector<std::shared_ptr<IndexConfig>>> indexes;
    autil::legacy::json::JsonMap settings;

private:
    void initSettings(const TableStructure *tableStructure);
    void initFileCompressSetting();
    void initTtlSetting(const TableStructure *tableStructure);
};

} // namespace catalog
