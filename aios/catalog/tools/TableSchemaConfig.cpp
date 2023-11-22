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
#include "catalog/tools/TableSchemaConfig.h"

#include "autil/result/Errors.h"

using namespace std;

namespace {
void setUpdatable(const map<string, const catalog::FieldConfig *> &fieldMap,
                  shared_ptr<catalog::AttributeConfig> attrConfig) {
    auto iter = fieldMap.find(attrConfig->fieldName);
    if (iter != fieldMap.end()) {
        attrConfig->updatable = iter->second->updatable;
    }
}
} // namespace

namespace catalog {
AUTIL_DECLARE_AND_SETUP_LOGGER(catalog, TableSchemaConfig);

static map<proto::TableStructure::Index::IndexType, string> indexTypeMap = {
    {proto::TableStructure::Index::NUMBER, "inverted_index"},
    {proto::TableStructure::Index::STRING, "inverted_index"},
    {proto::TableStructure::Index::TEXT, "inverted_index"},
    {proto::TableStructure::Index::EXPACK, "inverted_index"},
    {proto::TableStructure::Index::DATE, "inverted_index"},
    {proto::TableStructure::Index::RANGE, "inverted_index"},
    {proto::TableStructure::Index::SPATIAL, "inverted_index"},
    {proto::TableStructure::Index::CUSTOMIZED, "unsupported"},
    {proto::TableStructure::Index::SUMMARY, "summary"},
    {proto::TableStructure::Index::PACK, "inverted_index"},
    {proto::TableStructure::Index::PRIMARY_KEY, "primarykey"},
    {proto::TableStructure::Index::PRIMARY_KEY64, "primarykey"},
    {proto::TableStructure::Index::PRIMARY_KEY128, "primarykey"},
    {proto::TableStructure::Index::ORC, "orc"},
    {proto::TableStructure::Index::ATTRIBUTE, "attribute"},
    {proto::TableStructure::Index::ANN, "ann"},
    {proto::TableStructure::Index::SOURCE, "source"}};

std::vector<std::string> IndexConfig::intParams = {"term_payload_flag",
                                                   "doc_payload_flag",
                                                   "position_list_flag",
                                                   "position_payload_flag",
                                                   "term_frequency_flag",
                                                   "term_frequency_bitmap",
                                                   "format_version_id"};
std::vector<std::string> IndexConfig::boolParams = {"has_primary_key_attribute"};

class InvertedIndexConfigFactory {
public:
    InvertedIndexConfigFactory() {}
    ~InvertedIndexConfigFactory() {}

public:
    static IndexConfig *create(proto::TableStructure::Index::IndexType indexType) {
        switch (indexType) {
        case (proto::TableStructure::Index::DATE):
            return new DateIndexConfig();
        case (proto::TableStructure::Index::SPATIAL):
            return new SpatialIndexConfig();
        default:
            return new InvertedIndexConfig();
        }
    }
};

template <typename T>
bool getValueFromParams(const google::protobuf::Map<std::string, std::string> &params, std::string key, T &value) {
    auto valueIter = params.find(key);
    if (valueIter == params.end()) {
        AUTIL_LOG(DEBUG, "get [%s] from params failed", key.c_str());
        return false;
    }
    const auto &valueStr = valueIter->second;
    if (!autil::StringUtil::fromString(valueStr, value)) {
        AUTIL_LOG(ERROR, "convert [%s] to type [%s] failed", valueStr.c_str(), typeid(T).name());
        return false;
    }
    return true;
}

autil::Result<string> TableSchemaConfig::convertIndexType(proto::TableStructure::Index::IndexType indexType) {
    switch (indexType) {
    case (proto::TableStructure::Index::NUMBER):
        return "NUMBER";
    case (proto::TableStructure::Index::STRING):
        return "STRING";
    case (proto::TableStructure::Index::TEXT):
        return "TEXT";
    case (proto::TableStructure::Index::EXPACK):
        return "EXPACK";
    case (proto::TableStructure::Index::DATE):
        return "DATE";
    case (proto::TableStructure::Index::RANGE):
        return "RANGE";
    case (proto::TableStructure::Index::SPATIAL):
        return "SPATIAL";
    case (proto::TableStructure::Index::CUSTOMIZED):
        return "CUSTOMIZED";
    case (proto::TableStructure::Index::SUMMARY):
        return "SUMMARY";
    case (proto::TableStructure::Index::PACK):
        return "PACK";
    case (proto::TableStructure::Index::PRIMARY_KEY):
        return "PRIMARYKEY64";
    case (proto::TableStructure::Index::PRIMARY_KEY64):
        return "PRIMARYKEY64";
    case (proto::TableStructure::Index::PRIMARY_KEY128):
        return "PRIMARYKEY128";
    case (proto::TableStructure::Index::ORC):
        return "ORC";
    case (proto::TableStructure::Index::ATTRIBUTE):
        return "ATTRIBUTE";
    case (proto::TableStructure::Index::ANN):
        return "CUSTOMIZED";
    case (proto::TableStructure::Index::SOURCE):
        return "SOURCE";
    default:
        return autil::result::RuntimeError::make("not support index type %d", indexType);
    }
}

void FieldConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("field_name", fieldName);
    json.Jsonize("field_type", fieldType, fieldType);
    if (isMultiValue) {
        json.Jsonize("multi_value", isMultiValue, isMultiValue);
    }
    if (!defaultStrValue.empty()) {
        json.Jsonize("default_value", defaultStrValue, defaultStrValue);
    }
    if (supportNull) {
        json.Jsonize("enable_null", supportNull, supportNull);
    }
    if (!separator.empty()) {
        json.Jsonize("separator", separator, separator);
    }
    if (!analyzer.empty()) {
        json.Jsonize("analyzer", analyzer);
    } else if (fieldType == "TEXT") {
        json.Jsonize("analyzer", "simple_analyzer");
    }
}

bool FieldConfig::init(const proto::TableStructure::Column &column) {
    auto s = TableSchemaConfig::convertDataType(column.type());
    if (!s.is_ok()) {
        AUTIL_LOG(ERROR, "convert data type failed, error msg: %s", s.get_error().message().c_str());
        return false;
    }
    fieldName = column.name();
    fieldType = s.get();
    isMultiValue = column.multi_value();
    if (!column.default_value().empty()) {
        defaultStrValue = column.default_value();
    }
    supportNull = column.nullable();
    updatable = column.updatable();
    if (!column.separator().empty()) {
        separator = column.separator();
    }
    if (!column.analyzer().empty()) {
        analyzer = column.analyzer();
    }
    return true;
}

std::string IndexConfig::convertCompressType(proto::TableStructure::Index::IndexConfig::CompressType compressType) {
    switch (compressType) {
    case proto::TableStructure::Index::IndexConfig::SNAPPY:
        return "snappy";
    case proto::TableStructure::Index::IndexConfig::ZSTD:
        return "zstd";
    case proto::TableStructure::Index::IndexConfig::LZ4:
        return "lz4";
    case proto::TableStructure::Index::IndexConfig::LZ4HC:
        return "lz4hc";
    case proto::TableStructure::Index::IndexConfig::ZLIB:
        return "zlib";
    default:
        return "";
    }
}

void IndexConfig::jsonizeOneParam(autil::legacy::Jsonizable::JsonWrapper &json,
                                  const std::string &key,
                                  const std::string &value) {
    for (const auto &param : intParams) {
        if (param != key) {
            continue;
        }
        int64_t intValue = 0;
        if (!autil::StringUtil::numberFromString<int64_t>(value, intValue)) {
            std::string errMsg = "parse [" + value + "] to int64_t failed, key [" + key + "]";
            AUTIL_LOG(ERROR, "%s", errMsg.c_str());
            throw errMsg;
        }
        json.Jsonize(key, intValue);
        return;
    }
    for (const auto &param : boolParams) {
        if (param != key) {
            continue;
        }
        bool boolValue = false;
        if (!autil::StringUtil::parseTrueFalse(value, boolValue)) {
            std::string errMsg = "parse [" + value + "] to bool failed, key [" + key + "]";
            AUTIL_LOG(ERROR, "%s", errMsg.c_str());
            throw errMsg;
        }
        json.Jsonize(key, boolValue);
        return;
    }
    json.Jsonize(key, value);
}

void AttributeConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("field_name", fieldName, fieldName);
    if (!fileCompressor.empty()) {
        json.Jsonize("file_compressor", fileCompressor);
    }
    if (updatable) {
        json.Jsonize("updatable", updatable);
    }
    if (json.GetMode() == TO_JSON) {
        for (const auto &iter : indexParams) {
            jsonizeOneParam(json, iter.first, iter.second);
        }
    }
}
bool AttributeConfig::init(const proto::TableStructure::Index &index) {
    auto s = TableSchemaConfig::convertIndexType(index.index_type());
    if (!s.is_ok()) {
        AUTIL_LOG(ERROR, "convert index type failed, error msg: %s", s.get_error().message().c_str());
        return false;
    }
    indexType = s.get();

    if (index.index_config().index_fields_size() != 1) {
        AUTIL_LOG(ERROR,
                  "attribute fields size not equal 1, fieldName[%s]",
                  index.index_config().index_fields(0).field_name().c_str());
        return false;
    }

    fieldName = index.index_config().index_fields(0).field_name();
    fileCompressor = convertCompressType(index.index_config().compress_type());
    for (const auto &[first, second] : index.index_config().index_params()) {
        indexParams[first] = second;
    }

    return true;
}

void SummaryConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("summary_fields", fieldNames, fieldNames);
    if (!parameters.empty()) {
        json.Jsonize("parameter", parameters);
    }
}
bool SummaryConfig::init(const proto::TableStructure::Index &index) {
    auto s = TableSchemaConfig::convertIndexType(index.index_type());
    if (!s.is_ok()) {
        AUTIL_LOG(ERROR, "convert index type failed, error msg: %s", s.get_error().message().c_str());
        return false;
    }
    indexType = s.get();

    for (const auto &indexField : index.index_config().index_fields()) {
        fieldNames.push_back(indexField.field_name());
    }
    fileCompressor = convertCompressType(index.index_config().compress_type());
    if (!fileCompressor.empty()) {
        parameters["file_compressor"] = fileCompressor;
    }
    return true;
}

void PrimaryKeyIndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("index_name", indexName, indexName);
    json.Jsonize("index_type", indexType, indexType);
    json.Jsonize("index_fields", indexFields, indexFields);
    if (json.GetMode() == TO_JSON) {
        for (const auto &iter : indexParams) {
            jsonizeOneParam(json, iter.first, iter.second);
        }
    }
}

bool PrimaryKeyIndexConfig::init(const proto::TableStructure::Index &index) {
    auto s = TableSchemaConfig::convertIndexType(index.index_type());
    if (!s.is_ok()) {
        AUTIL_LOG(ERROR, "convert index type failed, error msg: %s", s.get_error().message().c_str());
        return false;
    }
    indexType = s.get();
    indexName = index.name();

    if (index.index_config().index_fields_size() != 1) {
        AUTIL_LOG(ERROR, "single field index index fields size not equal 1");
        return false;
    }

    indexFields = index.index_config().index_fields(0).field_name();
    fileCompressor = convertCompressType(index.index_config().compress_type());
    if (!fileCompressor.empty()) {
        AUTIL_LOG(ERROR,
                  "primary key index [%s] does not support file compress, [%s]",
                  indexName.c_str(),
                  fileCompressor.c_str());
        return false;
    }
    for (const auto &[first, second] : index.index_config().index_params()) {
        indexParams[first] = second;
    }
    return true;
}

void OrcIndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("index_name", indexName, indexName);
    json.Jsonize("index_type", indexType, indexType);
    json.Jsonize("index_fields", indexFields, indexFields);
}

bool OrcIndexConfig::init(const proto::TableStructure::Index &index) {
    indexName = "__orc_default__";
    auto s = TableSchemaConfig::convertIndexType(index.index_type());
    if (!s.is_ok()) {
        AUTIL_LOG(ERROR, "convert index type failed, error msg: %s", s.get_error().message().c_str());
        return false;
    }
    indexType = s.get();

    for (const auto &indexField : index.index_config().index_fields()) {
        indexFields.push_back(indexField.field_name());
    }

    fileCompressor = convertCompressType(index.index_config().compress_type());
    if (!fileCompressor.empty()) {
        AUTIL_LOG(
            ERROR, "orc index [%s] does not support file compress, [%s]", indexName.c_str(), fileCompressor.c_str());
        return false;
    }

    return true;
}

void PackField::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("field_name", fieldName, fieldName);
    json.Jsonize("boost", boost, boost);
}

bool InvertedIndexConfig::hasMultiFields(const string &indexType) {
    if (indexType == "PACK" || indexType == "EXPACK" || indexType == "CUSTOMIZED") {
        return true;
    }
    return false;
}

void InvertedIndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("index_name", indexName, indexName);
    json.Jsonize("index_type", indexType, indexType);
    if (hasMultiFields(indexType)) {
        json.Jsonize("index_fields", packIndexFields, packIndexFields);
    } else {
        json.Jsonize("index_fields", indexFields, indexFields);
    }
    if (json.GetMode() == TO_JSON) {
        for (const auto &iter : indexParams) {
            jsonizeOneParam(json, iter.first, iter.second);
        }
        if (!parameters.empty()) {
            json.Jsonize("parameters", parameters);
        };
    }
    if (!fileCompressor.empty()) {
        json.Jsonize("file_compressor", fileCompressor);
    }
}

bool InvertedIndexConfig::init(const catalog::proto::TableStructure::Index &index) {
    indexName = index.name();
    auto s = TableSchemaConfig::convertIndexType(index.index_type());
    if (!s.is_ok()) {
        AUTIL_LOG(ERROR, "convert index type failed, error msg: %s", s.get_error().message().c_str());
        return false;
    }
    indexType = s.get();

    if (hasMultiFields(indexType)) {
        for (const auto &indexField : index.index_config().index_fields()) {
            PackField field;
            field.fieldName = indexField.field_name();
            field.boost = indexField.boost();
            packIndexFields.emplace_back(field);
        }
    } else {
        if (index.index_config().index_fields_size() != 1) {
            AUTIL_LOG(ERROR, "un pack inverted index index fields size not equal 1, indexName[%s]", indexName.c_str());
            return false;
        }
        indexFields = index.index_config().index_fields(0).field_name();
    }
    for (const auto &[first, second] : index.index_config().index_params()) {
        if (first != "parameters") {
            indexParams[first] = second;
            continue;
        }
        try {
            autil::legacy::FromJsonString(parameters, second);
        } catch (const std::exception &e) {
            AUTIL_LOG(ERROR, "parse parameter failed, [%s] exception [%s]", second.c_str(), e.what());
            return false;
        }
    }
    fileCompressor = convertCompressType(index.index_config().compress_type());
    return true;
}

void TableSchemaConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    if (json.GetMode() == TO_JSON) {
        json.Jsonize("table_name", tableName, tableName);
        json.Jsonize("table_type", tableType, tableType);
        json.Jsonize("fields", fields, fields);
        map<string, vector<autil::legacy::Any>> anyConfigs;
        for (const auto &[indexType, configs] : indexes) {
            auto &anys = anyConfigs[indexType];
            for (const auto &config : configs) {
                anys.push_back(autil::legacy::ToJson(config));
            }
        }
        json.Jsonize("indexes", anyConfigs);
        if (!settings.empty()) {
            json.Jsonize("settings", settings);
        }
    }
}

bool TableSchemaConfig::init(const string &name, const TableStructure *tableStructure) {
    tableName = name;
    auto s = ConvertTableType(tableStructure->tableType());
    if (!s.is_ok()) {
        AUTIL_LOG(ERROR, "conver table type failed, error msg: %s", s.get_error().message().c_str());
        return false;
    }
    tableType = s.get();
    for (const auto &column : tableStructure->columns()) {
        FieldConfig field;
        if (!field.init(column)) {
            AUTIL_LOG(ERROR, "field init failed, name[%s]", column.name().c_str());
            return false;
        }
        fields.emplace_back(field);
    }

    map<string, const FieldConfig *> fieldMap;
    for (const auto &field : fields) {
        fieldMap[field.fieldName] = &field;
    }

    if (tableType == "kv") {
        AUTIL_LOG(ERROR, "not supported kv table now");
        return false;
    } else if (tableType == "kkv") {
        AUTIL_LOG(ERROR, "not supported kkv table now");
        return false;
    } else if (tableType == "orc") {
        auto &orcIndexConfigs = indexes["orc"];
        auto &pkIndexConfigs = indexes["primarykey"];
        for (const auto &index : tableStructure->indexes()) {
            const auto &indexType = indexTypeMap[index.index_type()];
            if (indexType == "orc") {
                auto indexConfig = make_shared<OrcIndexConfig>();
                if (!indexConfig->init(index)) {
                    return false;
                }
                orcIndexConfigs.emplace_back(indexConfig);
            } else if (indexType == "primarykey") {
                if (pkIndexConfigs.size() > 0) {
                    AUTIL_LOG(ERROR, "can only have one primary key config");
                    return false;
                }
                auto indexConfig = make_shared<PrimaryKeyIndexConfig>();
                if (!indexConfig->init(index)) {
                    return false;
                }
                pkIndexConfigs.emplace_back(indexConfig);
            } else {
                AUTIL_LOG(ERROR, "not supported index type: %s", indexType.c_str());
                return false;
            }
        }
    } else if (tableType == "normal") {
        for (const auto &index : tableStructure->indexes()) {
            auto iter = indexTypeMap.find(index.index_type());
            if (iter == indexTypeMap.end()) {
                AUTIL_LOG(ERROR,
                          "can not find [%d] from indexTypeMap, indexName[%s]",
                          index.index_type(),
                          index.name().c_str());
                return false;
            }
            const auto &indexType = iter->second;
            if (indexType == "primarykey") {
                if (indexes["primarykey"].size() > 0) {
                    AUTIL_LOG(ERROR, "can only have one primary key config");
                    return false;
                }
                auto indexConfig = make_shared<PrimaryKeyIndexConfig>();
                if (!indexConfig->init(index)) {
                    return false;
                }
                indexes["primarykey"].emplace_back(indexConfig);
            } else if (indexType == "inverted_index") {
                std::shared_ptr<IndexConfig> indexConfig(InvertedIndexConfigFactory::create(index.index_type()));
                if (!indexConfig->init(index)) {
                    return false;
                }
                indexes["inverted_index"].emplace_back(indexConfig);
            } else if (indexType == "summary") {
                auto summaryConfig = make_shared<SummaryConfig>();
                if (!summaryConfig->init(index)) {
                    return false;
                }
                indexes["summary"].emplace_back(summaryConfig);
            } else if (indexType == "attribute") {
                auto attrConfig = make_shared<AttributeConfig>();
                if (!attrConfig->init(index)) {
                    return false;
                }
                setUpdatable(fieldMap, attrConfig);
                indexes["attribute"].emplace_back(attrConfig);
            } else if (indexType == "ann") {
                auto annConfig = make_shared<InvertedIndexConfig>();
                if (!annConfig->init(index)) {
                    return false;
                }
                indexes["ann"].emplace_back(annConfig);
            } else if (indexType == "source") {
                auto sourceConfig = make_shared<SourceIndexConfig>();
                if (!sourceConfig->init(index)) {
                    return false;
                }
                indexes["source"].emplace_back(sourceConfig);
            } else {
                AUTIL_LOG(ERROR, "not supported index type category: %s", indexType.c_str());
                return false;
            }
        }
    } else {
        AUTIL_LOG(ERROR, "not supported table type: %s", tableType.c_str());
        return false;
    }
    initSettings(tableStructure);

    return true;
}

autil::Result<string> TableSchemaConfig::convertDataType(proto::TableStructure::Column::DataType dataType) {
    switch (dataType) {
    case (proto::TableStructure::Column::INT8):
        return "INT8";
    case (proto::TableStructure::Column::INT16):
        return "INT16";
    case (proto::TableStructure::Column::INT32):
        return "INTEGER";
    case (proto::TableStructure::Column::INT64):
        return "INT64";
    case (proto::TableStructure::Column::UINT8):
        return "UINT8";
    case (proto::TableStructure::Column::UINT16):
        return "UINT16";
    case (proto::TableStructure::Column::UINT32):
        return "UINT32";
    case (proto::TableStructure::Column::UINT64):
        return "UINT64";
    case (proto::TableStructure::Column::FLOAT):
        return "FLOAT";
    case (proto::TableStructure::Column::DOUBLE):
        return "DOUBLE";
    case (proto::TableStructure::Column::STRING):
        return "STRING";
    case (proto::TableStructure::Column::TEXT):
        return "TEXT";
    case (proto::TableStructure::Column::RAW):
        return "RAW";
    case (proto::TableStructure::Column::TIME):
        return "TIME";
    case (proto::TableStructure::Column::TIMESTAMP):
        return "TIMESTAMP";
    case (proto::TableStructure::Column::DATE):
        return "DATE";
    case (proto::TableStructure::Column::LOCATION):
        return "LOCATION";
    case (proto::TableStructure::Column::POLYGON):
        return "POLYGON";
    case (proto::TableStructure::Column::LINE):
        return "LINE";
    default:
        return autil::result::RuntimeError::make("not support data type %d", dataType);
    }
}

autil::Result<string> TableSchemaConfig::ConvertTableType(proto::TableType::Code code) {
    switch (code) {
    case (proto::TableType::NORMAL):
        return "normal";
    case (proto::TableType::KV):
        return "kv";
    case (proto::TableType::KKV):
        return "kkv";
    case (proto::TableType::ORC):
        return "orc";
    default:
        return autil::result::RuntimeError::make("not support table type %d", code);
    }
}

void TableSchemaConfig::initFileCompressSetting() {
    bool hasFileCompress = false;
    for (const auto &[indexType, indexConfigs] : indexes) {
        for (const auto &indexConfig : indexConfigs) {
            if (!indexConfig->fileCompressor.empty()) {
                hasFileCompress = true;
                break;
            }
        }
        if (hasFileCompress) {
            break;
        }
    }
    if (!hasFileCompress) {
        return;
    }
    vector<map<string, string>> fileCompressors;
    fileCompressors.push_back({{"name", "snappy"}, {"type", "snappy"}});
    fileCompressors.push_back({{"name", "zstd"}, {"type", "zstd"}});
    fileCompressors.push_back({{"name", "lz4"}, {"type", "lz4"}});
    fileCompressors.push_back({{"name", "lz4hc"}, {"type", "lz4hc"}});
    fileCompressors.push_back({{"name", "zlib"}, {"type", "zlib"}});
    settings["file_compressors"] = autil::legacy::ToJson(fileCompressors);
}

void TableSchemaConfig::initTtlSetting(const TableStructure *tableStructure) {
    const auto &ttlOption = tableStructure->tableStructureConfig().ttl_option();
    if (!ttlOption.enable_ttl()) {
        return;
    }
    settings["enable_ttl"] = true;
    if (!ttlOption.ttl_field_name().empty()) {
        settings["ttl_field_name"] = ttlOption.ttl_field_name();
    }
    if (ttlOption.default_ttl() > 0) {
        settings["default_ttl"] = ttlOption.default_ttl();
    }
}

void TableSchemaConfig::initTableMetaOption(const TableStructure *tableStructure) {
    const auto &tableMetaOption = tableStructure->tableStructureConfig().table_meta_option();

    if (tableMetaOption.enable_all_text_field_meta()) {
        settings["enable_all_text_field_meta"] = true;
    }
}


void TableSchemaConfig::initSettings(const TableStructure *tableStructure) {
    initFileCompressSetting();
    initTtlSetting(tableStructure);
    initTableMetaOption(tableStructure);
}

void SourceIndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    std::vector<std::map<std::string, std::string>> groupConfig;
    std::map<std::string, std::string> singleGroupConfig;
    singleGroupConfig["field_mode"] = fieldMode;
    groupConfig.push_back(singleGroupConfig);
    json.Jsonize("group_configs", groupConfig);
}

bool SourceIndexConfig::init(const catalog::proto::TableStructure::Index &index) {
    auto s = TableSchemaConfig::convertIndexType(index.index_type());
    if (!s.is_ok()) {
        AUTIL_LOG(ERROR, "convert index type failed, error msg: %s", s.get_error().message().c_str());
        return false;
    }
    indexType = s.get();
    const auto &params = index.index_config().index_params();
    auto fieldModeIter = params.find("field_mode");
    if (fieldModeIter == params.end()) {
        AUTIL_LOG(ERROR, "get field mode from params failed");
        return false;
    }
    fieldMode = fieldModeIter->second;
    if (fieldMode != "all_field") {
        AUTIL_LOG(ERROR, "only support \"all_field\" now");
        return false;
    }
    return true;
}

void DateIndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("index_name", indexName, indexName);
    json.Jsonize("index_type", indexType, indexType);
    json.Jsonize("index_fields", indexFields, indexFields);
    json.Jsonize("build_granularity", buildGranularity, buildGranularity);
    if (!fileCompressor.empty()) {
        json.Jsonize("file_compressor", fileCompressor);
    }
}

bool DateIndexConfig::init(const catalog::proto::TableStructure::Index &index) {
    auto s = TableSchemaConfig::convertIndexType(index.index_type());
    if (!s.is_ok()) {
        AUTIL_LOG(ERROR, "convert index type failed, error msg: %s", s.get_error().message().c_str());
        return false;
    }
    indexType = s.get();
    indexName = index.name();
    if (index.index_config().index_fields_size() != 1) {
        AUTIL_LOG(ERROR, "un pack inverted index index fields size not equal 1, indexName[%s]", indexName.c_str());
        return false;
    }
    indexFields = index.index_config().index_fields(0).field_name();
    const auto &params = index.index_config().index_params();
    if (!getValueFromParams(params, "build_granularity", buildGranularity)) {
        AUTIL_LOG(ERROR, "get build granularity from params failed");
        return false;
    }
    const set<string> granularities = {"year", "month", "day", "hour", "minute", "second", "millisecond"};
    if (granularities.find(buildGranularity) == granularities.end()) {
        AUTIL_LOG(ERROR, "not support build granularity value [%s]", buildGranularity.c_str());
        return false;
    }
    fileCompressor = convertCompressType(index.index_config().compress_type());
    return true;
}

void SpatialIndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("index_name", indexName, indexName);
    json.Jsonize("index_type", indexType, indexType);
    json.Jsonize("index_fields", indexFields, indexFields);
    json.Jsonize("max_search_dist", maxSearchDist, maxSearchDist);
    json.Jsonize("max_dist_err", maxDistErr, maxDistErr);
    if (distanceLossAccuracy) {
        json.Jsonize("distance_loss_accuracy", distanceLossAccuracy.value(), distanceLossAccuracy.value());
    }
    if (!fileCompressor.empty()) {
        json.Jsonize("file_compressor", fileCompressor);
    }
}

bool SpatialIndexConfig::init(const catalog::proto::TableStructure::Index &index) {
    auto s = TableSchemaConfig::convertIndexType(index.index_type());
    if (!s.is_ok()) {
        AUTIL_LOG(ERROR, "convert index type failed, error msg: %s", s.get_error().message().c_str());
        return false;
    }
    indexType = s.get();
    indexName = index.name();
    if (index.index_config().index_fields_size() != 1) {
        AUTIL_LOG(ERROR, "un pack inverted index index fields size not equal 1, indexName[%s]", indexName.c_str());
        return false;
    }
    indexFields = index.index_config().index_fields(0).field_name();
    const auto &params = index.index_config().index_params();
    if (!getValueFromParams(params, "max_search_dist", maxSearchDist)) {
        AUTIL_LOG(ERROR, "get max_search_dist from params failed");
        return false;
    }
    if (!getValueFromParams(params, "max_dist_err", maxDistErr)) {
        AUTIL_LOG(ERROR, "get max_dist_err from params failed");
        return false;
    }
    double distanceLossAccuracyVal = 0.025;
    if (!getValueFromParams(params, "distance_loss_accuracy", distanceLossAccuracyVal)) {
        AUTIL_LOG(WARN, "get distance_loss_accuracy from params failed, remain empty(use default value)");
    } else {
        distanceLossAccuracy.emplace(distanceLossAccuracyVal);
    }
    fileCompressor = convertCompressType(index.index_config().compress_type());
    return true;
}

} // namespace catalog
