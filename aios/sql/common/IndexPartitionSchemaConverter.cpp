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
#include "sql/common/IndexPartitionSchemaConverter.h"

#include <algorithm>
#include <assert.h>
#include <iosfwd>
#include <map>
#include <utility>

#include "autil/StringUtil.h"
#include "autil/legacy/json.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/customized_config.h"
#include "indexlib/config/customized_table_config.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/config/SingleFieldIndexConfig.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"
#include "iquan/common/Common.h"
#include "iquan/common/catalog/IndexDef.h"
#include "iquan/common/catalog/TableDef.h"


using namespace std;
using autil::legacy::json::JsonMap;

using namespace indexlib::config;
using namespace iquan;

namespace sql {
AUTIL_LOG_SETUP(ha3, IndexPartitionSchemaConverter);

IndexPartitionSchemaConverter::IndexPartitionSchemaConverter() {}

IndexPartitionSchemaConverter::~IndexPartitionSchemaConverter() {}

bool IndexPartitionSchemaConverter::convert(
    const std::shared_ptr<indexlibv2::config::ITabletSchema> &tableSchema, TableDef &tableDef) {
    const auto &legacySchemaAdapter = std::dynamic_pointer_cast<LegacySchemaAdapter>(tableSchema);
    if (!legacySchemaAdapter) {
        // indexlibv2
        return convertV2(tableSchema, tableDef);
    }
    const IndexPartitionSchemaPtr &schema = legacySchemaAdapter->GetLegacySchema();
    const auto &tableType = IndexPartitionSchema::TableType2Str(schema->GetTableType());
    tableDef.tableName = schema->GetSchemaName();
    tableDef.tableType = tableType;

    auto &fieldSchema = schema->GetFieldSchema();
    if (!fieldSchema) {
        return true;
    }
    if (isKhronosTable(tableSchema)) {
        return convertLegacyKhronosSchema(schema, tableDef);
    } else if (tableType == IQUAN_TABLE_TYPE_KKV || tableType == IQUAN_TABLE_TYPE_KV
               || tableType == IQUAN_TABLE_TYPE_ORC) {
        return convertNonInvertedTable(schema, tableType, tableDef);
    } else if (tableType == IQUAN_TABLE_TYPE_NORMAL) {
        return convertNormalTable(schema, tableType, tableDef);
    } else {
        AUTIL_LOG(ERROR, "not support table type: %s", tableType.c_str());
        return false;
    }
    return true;
}

bool IndexPartitionSchemaConverter::convertV2(
    const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema, TableDef &tableDef) {
    const auto &tableType = schema->GetTableType();
    tableDef.tableName = schema->GetTableName();
    tableDef.tableType = tableType;

    const auto &fieldConfigs = schema->GetFieldConfigs();
    if (fieldConfigs.empty()) {
        return true;
    }


    if (tableType == IQUAN_TABLE_TYPE_KKV || tableType == IQUAN_TABLE_TYPE_KV
        || tableType == IQUAN_TABLE_TYPE_ORC || tableType == IQUAN_TABLE_TYPE_AGGREGATION) {
        return convertNonInvertedTable(schema, tableType, tableDef);
    } else if (tableType == IQUAN_TABLE_TYPE_NORMAL) {
        return convertNormalTable(schema, tableType, tableDef);
    } else if (isKhronosTable(schema)) {
        return convertKhronosTable(schema, tableDef);
    } else {
        AUTIL_LOG(ERROR, "not support table type: %s", tableType.c_str());
        return false;
    }
    return true;
}

bool IndexPartitionSchemaConverter::isKhronosTable(
    const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema) {
    const auto &tableType = schema->GetTableType();
    if (tableType == IQUAN_TABLE_TYPE_KHRONOS) {
        return true;
    }
    if (tableType != IQUAN_TABLE_TYPE_CUSTOMIZED) {
        return false;
    }
    auto legacySchema = schema->GetLegacySchema();
    if (legacySchema == nullptr) {
        return false;
    }
    const auto &customTableConfig = legacySchema->GetCustomizedTableConfig();
    if (customTableConfig != nullptr && customTableConfig->GetIdentifier() == "khronos") {
        return true;
    }
    return false;
}

bool IndexPartitionSchemaConverter::convertKhronosTable(
    const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema, TableDef &tableDef) {
    const auto &tableType = schema->GetTableType();
    if (tableType == IQUAN_TABLE_TYPE_KHRONOS) {
        return true;
    }
    return convertLegacyKhronosSchema(schema->GetLegacySchema(), tableDef);
}


bool IndexPartitionSchemaConverter::convertNormalTable(const IndexPartitionSchemaPtr &schema,
                                                       const std::string &tableType,
                                                       TableDef &tableDef) {
    auto &fieldSchema = schema->GetFieldSchema();
    auto &indexSchema = schema->GetIndexSchema();
    auto &attrSchema = schema->GetAttributeSchema();
    auto &summarySchema = schema->GetSummarySchema();

    // fill fields
    convertFields(fieldSchema, indexSchema, attrSchema, summarySchema, tableType, tableDef.fields);

    // fill summary fields
    if (summarySchema && summarySchema->GetSummaryCount() > 0) {
        convertFields(fieldSchema,
                      indexSchema,
                      attrSchema,
                      summarySchema,
                      IQUAN_TABLE_TYPE_SUMMARY,
                      tableDef.summaryFields);
    }

    // fill sub table
    auto &subSchemaPtr = schema->GetSubIndexPartitionSchema();
    if (subSchemaPtr) {
        SubTableDef subTableDef;
        subTableDef.subTableName = subSchemaPtr->GetSchemaName();
        convertFields(subSchemaPtr->GetFieldSchema(),
                      subSchemaPtr->GetIndexSchema(),
                      subSchemaPtr->GetAttributeSchema(),
                      nullptr,
                      tableType,
                      subTableDef.subFields);
        tableDef.subTables.emplace_back(std::move(subTableDef));
    }
    return true;
}

bool IndexPartitionSchemaConverter::convertNormalTable(
    const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema,
    const std::string &tableType,
    TableDef &tableDef) {
    // fill fields
    convertFields(schema, tableType, tableDef.fields);

    // fill summary fields
    const auto &indexConfigs = schema->GetIndexConfigs(indexlibv2::index::SUMMARY_INDEX_TYPE_STR);
    if (!indexConfigs.empty()) {
        assert(indexConfigs.size() == 1u);
        const auto &summaryConfig
            = std::dynamic_pointer_cast<indexlibv2::config::SummaryIndexConfig>(indexConfigs[0]);
        assert(summaryConfig);
        if (summaryConfig->GetSummaryCount() > 0) {
            convertFields(schema, IQUAN_TABLE_TYPE_SUMMARY, tableDef.summaryFields);
        }
    }
    return true;
}

bool IndexPartitionSchemaConverter::convertLegacyKhronosSchema(
    const IndexPartitionSchemaPtr &schema, TableDef &tableDef) {
    if (schema == nullptr) {
        AUTIL_LOG(ERROR, "convert legacy khronos schema error, schema is null");
        return false;
    }
    auto &fieldSchema = schema->GetFieldSchema();
    auto &indexSchema = schema->GetIndexSchema();
    auto &attrSchema = schema->GetAttributeSchema();
    auto &customTableConfig = schema->GetCustomizedTableConfig();

    if (!customTableConfig) {
        AUTIL_LOG(ERROR, "customized table expect config, table [%s]", tableDef.tableName.c_str());
        return false;
    }
    const auto &identifier = customTableConfig->GetIdentifier();
    if (identifier == CUSTOM_TABLE_KHRONOS) {
        tableDef.properties[CUSTOM_IDENTIFIER] = CUSTOM_TABLE_KHRONOS;
        const auto &parameters = customTableConfig->GetParameters();
        tableDef.properties.insert(parameters.begin(), parameters.end());
    }
    convertFields(
        fieldSchema, indexSchema, attrSchema, nullptr, CUSTOM_TABLE_KHRONOS, tableDef.fields);
    return true;
}

bool IndexPartitionSchemaConverter::convertNonInvertedTable(const IndexPartitionSchemaPtr &schema,
                                                            const std::string &tableType,
                                                            TableDef &tableDef) {
    auto &fieldSchema = schema->GetFieldSchema();
    auto &indexSchema = schema->GetIndexSchema();
    auto &attrSchema = schema->GetAttributeSchema();

    convertFields(fieldSchema, indexSchema, attrSchema, nullptr, tableType, tableDef.fields);
    return true;
}

bool IndexPartitionSchemaConverter::convertNonInvertedTable(
    const shared_ptr<indexlibv2::config::ITabletSchema> &schema,
    const std::string &tableType,
    TableDef &tableDef) {
    convertFields(schema, tableType, tableDef.fields);
    return true;
}

void IndexPartitionSchemaConverter::convertIndexFieldConfig(const IndexSchemaPtr &indexSchema,
                                                            const std::string &tableType,
                                                            FieldDef &fieldDef) {
    if (!indexSchema) {
        return;
    }
    if (tableType == IQUAN_TABLE_TYPE_NORMAL || tableType == IQUAN_TABLE_TYPE_SUMMARY
        || tableType == CUSTOM_TABLE_KHRONOS) {
        auto indexConfig = indexSchema->GetIndexConfig(fieldDef.fieldName);
        if (indexConfig) {
            fieldDef.indexName = indexConfig->GetIndexName();
            fieldDef.indexType
                = IndexConfig::InvertedIndexTypeToStr(indexConfig->GetInvertedIndexType());
        }
    } else if (tableType == IQUAN_TABLE_TYPE_KKV) {
        auto singleFieldIndexConfig = indexSchema->GetPrimaryKeyIndexConfig();
        if (!singleFieldIndexConfig) {
            return;
        }
        auto kkvIndexConfig
            = std::dynamic_pointer_cast<indexlib::config::KKVIndexConfig>(singleFieldIndexConfig);
        if (!kkvIndexConfig) {
            return;
        }
        const auto &pkName = kkvIndexConfig->GetPrefixFieldName();
        const auto &skName = kkvIndexConfig->GetSuffixFieldName();
        if (fieldDef.fieldName == pkName) {
            fieldDef.indexName = pkName;
            fieldDef.indexType = std::string(KKV_PRIMARY_KEY);
        } else if (fieldDef.fieldName == skName) {
            fieldDef.indexName = skName;
            fieldDef.indexType = std::string(KKV_SECONDARY_KEY);
        }
    } else if (tableType == IQUAN_TABLE_TYPE_KV || tableType == IQUAN_TABLE_TYPE_ORC) {
        auto singleFieldIndexConfig = indexSchema->GetPrimaryKeyIndexConfig();
        if (!singleFieldIndexConfig) {
            return;
        }
        const auto &pkIndexFieldName = indexSchema->GetPrimaryKeyIndexFieldName();
        if (pkIndexFieldName == fieldDef.fieldName) {
            fieldDef.indexName = singleFieldIndexConfig->GetIndexName();
            fieldDef.indexType
                = IndexConfig::InvertedIndexTypeToStr(indexSchema->GetPrimaryKeyIndexType());
        }
    }
}

void IndexPartitionSchemaConverter::convertIndexFieldConfig(
    const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema,
    const std::string &tableType,
    FieldDef &fieldDef) {
    if (!schema) {
        return;
    }
    if (tableType == IQUAN_TABLE_TYPE_NORMAL || tableType == IQUAN_TABLE_TYPE_SUMMARY) {
        auto indexConfig = schema->GetIndexConfig(indexlib::index::GENERAL_INVERTED_INDEX_TYPE_STR,
                                                  fieldDef.fieldName);
        if (indexConfig) {
            const auto &invertedConfig
                = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
            assert(invertedConfig);
            fieldDef.indexName = invertedConfig->GetIndexName();
            fieldDef.indexType = indexlibv2::config::InvertedIndexConfig::InvertedIndexTypeToStr(
                invertedConfig->GetInvertedIndexType());
        } else {
            const auto &config = schema->GetPrimaryKeyIndexConfig();
            if (config && config->GetIndexName() == fieldDef.fieldName) {
                const auto &pkConfig
                    = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(config);
                assert(pkConfig);
                fieldDef.indexName = pkConfig->GetIndexName();
                fieldDef.indexType
                    = indexlibv2::config::InvertedIndexConfig::InvertedIndexTypeToStr(
                        pkConfig->GetInvertedIndexType());
            }
        }
    } else if (tableType == IQUAN_TABLE_TYPE_KKV) {
        // indexlibv2 does not support kkv yet
        assert(0);
    } else if (tableType == IQUAN_TABLE_TYPE_KV || tableType == IQUAN_TABLE_TYPE_AGGREGATION) {
        const auto &indexConfig = schema->GetPrimaryKeyIndexConfig();
        if (indexConfig) {
            const auto &kvConfig
                = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfig);
            assert(kvConfig);
            if (kvConfig->GetKeyFieldName() == fieldDef.fieldName) {
                fieldDef.indexName = kvConfig->GetIndexName();
                fieldDef.indexType
                    = indexlibv2::config::InvertedIndexConfig::InvertedIndexTypeToStr(it_kv);
            }
        }
    } else if (tableType == IQUAN_TABLE_TYPE_ORC) {
        const auto &indexConfig = schema->GetPrimaryKeyIndexConfig();
        if (indexConfig) {
            const auto &pkConfig
                = std::dynamic_pointer_cast<indexlibv2::config::SingleFieldIndexConfig>(
                    indexConfig);
            assert(pkConfig);
            if (pkConfig->GetFieldConfig()->GetFieldName() == fieldDef.fieldName) {
                fieldDef.indexName = indexConfig->GetIndexName();
                fieldDef.indexType
                    = indexlibv2::config::InvertedIndexConfig::InvertedIndexTypeToStr(
                        pkConfig->GetInvertedIndexType());
            }
        }
    } else {
        assert(0);
        AUTIL_LOG(ERROR, "unsupported table type [%s]", tableType.c_str());
    }
}

bool IndexPartitionSchemaConverter::convertFields(const FieldSchemaPtr &fieldSchema,
                                                  const IndexSchemaPtr &indexSchema,
                                                  const AttributeSchemaPtr &attrSchema,
                                                  const SummarySchemaPtr &summarySchema,
                                                  const std::string &tableType,
                                                  std::vector<FieldDef> &fields) {
    auto fieldConfigPtrIterator = fieldSchema->Begin();
    for (; fieldConfigPtrIterator != fieldSchema->End(); ++fieldConfigPtrIterator) {
        const auto &fieldConfigPtr = *fieldConfigPtrIterator;
        if (fieldConfigPtr->IsBuiltInField()) {
            continue;
        }
        FieldDef fieldDef;
        // 1. store the field name
        fieldDef.fieldName = fieldConfigPtr->GetFieldName();
        auto fieldId = fieldConfigPtr->GetFieldId();

        // 2. store the field index info
        convertIndexFieldConfig(indexSchema, tableType, fieldDef);

        // 3. check attribute and summary
        bool hasAttribute = attrSchema && attrSchema->IsInAttribute(fieldId);
        fieldDef.isAttr = hasAttribute;

        bool isSummaryField = false;
        if (isNormalPkIndex(fieldDef.indexType)) {
            isSummaryField = true;
        } else if (hasAttribute) {
            isSummaryField = true;
        } else {
            if (summarySchema && summarySchema->GetSummaryConfig(fieldDef.fieldName)) {
                isSummaryField = true;
            }
        }

        // 4. check fields
        if (tableType == IQUAN_TABLE_TYPE_NORMAL || tableType == IQUAN_TABLE_TYPE_KKV
            || tableType == IQUAN_TABLE_TYPE_KV || tableType == IQUAN_TABLE_TYPE_ORC) {
            if (fieldDef.indexType.empty() && !hasAttribute && !isSummaryField) {
                continue;
            }
        }

        if (tableType == IQUAN_TABLE_TYPE_SUMMARY) {
            if (!isSummaryField) {
                continue;
            }
        }

        // 5. store the field other info
        std::string fieldType = FieldConfig::FieldTypeToStr(fieldConfigPtr->GetFieldType());
        convertFieldType(fieldType, fieldConfigPtr->IsMultiValue(), fieldDef.fieldType);

        // 6. check valid
        if (!fieldDef.isValid() && tableType != CUSTOM_TABLE_KHRONOS) {
            AUTIL_LOG(WARN, "field [%s] is not valid", fieldDef.fieldName.c_str());
            return false;
        }
        fields.emplace_back(fieldDef);
    }
    return true;
}

void IndexPartitionSchemaConverter::convertFieldType(const std::string &fieldType,
                                                     bool isMultiValue,
                                                     iquan::FieldTypeDef &fieldTypeDef) {
    if (isCustomizedFieldTypeToDoubleArray(fieldType)) {
        fieldTypeDef.valueType["type"] = std::string("DOUBLE");
        fieldTypeDef.typeName = std::string("ARRAY");
    } else if (isCustomizedFieldTypeToString(fieldType)) {
        fieldTypeDef.typeName = std::string("STRING");
    } else {
        if (isMultiValue) {
            fieldTypeDef.valueType["type"] = fieldType;
            fieldTypeDef.typeName = std::string("ARRAY");
        } else {
            fieldTypeDef.typeName = fieldType;
        }
    }
}

bool IndexPartitionSchemaConverter::convertFields(
    const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema,
    const std::string &tableType,
    std::vector<iquan::FieldDef> &fields) {
    const auto &attrFields
        = schema->GetIndexFieldConfigs(indexlib::index::GENERAL_VALUE_INDEX_TYPE_STR);
    auto isAttrField = [&attrFields](const std::string &fieldName) {
        for (const auto &fieldConfig : attrFields) {
            if (fieldConfig->GetFieldName() == fieldName) {
                return true;
            }
        }
        return false;
    };
    const auto &fieldConfigs = schema->GetFieldConfigs();
    for (const auto &fieldConfigPtr : fieldConfigs) {
        if (fieldConfigPtr->IsVirtual()) {
            continue;
        }
        FieldDef fieldDef;
        // 1. store the field name
        const auto &fieldName = fieldConfigPtr->GetFieldName();
        fieldDef.fieldName = fieldName;

        // 2. store the field index info
        convertIndexFieldConfig(schema, tableType, fieldDef);

        // 3. check attribute and summary
        bool hasAttribute = isAttrField(fieldName);
        fieldDef.isAttr = hasAttribute;

        bool isSummaryField = false;
        if (isNormalPkIndex(fieldDef.indexType)) {
            isSummaryField = true;
        } else if (hasAttribute) {
            isSummaryField = true;
        } else {
            const auto &indexConfigs
                = schema->GetIndexConfigs(indexlibv2::index::SUMMARY_INDEX_TYPE_STR);
            if (!indexConfigs.empty()) {
                assert(indexConfigs.size() == 1u);
                const auto &summaryConfig
                    = std::dynamic_pointer_cast<indexlibv2::config::SummaryIndexConfig>(
                        indexConfigs[0]);
                assert(summaryConfig);
                if (summaryConfig->GetSummaryConfig(fieldName)) {
                    isSummaryField = true;
                }
            }
        }

        // 4. check fields
        if (tableType == IQUAN_TABLE_TYPE_NORMAL || tableType == IQUAN_TABLE_TYPE_KKV
            || tableType == IQUAN_TABLE_TYPE_KV || tableType == IQUAN_TABLE_TYPE_ORC) {
            if (fieldDef.indexType.empty() && !hasAttribute && !isSummaryField) {
                continue;
            }
        }

        if (tableType == IQUAN_TABLE_TYPE_SUMMARY) {
            if (!isSummaryField) {
                continue;
            }
        }

        // 5. store the field other info
        std::string fieldType
            = indexlibv2::config::FieldConfig::FieldTypeToStr(fieldConfigPtr->GetFieldType());
        convertFieldType(fieldType, fieldConfigPtr->IsMultiValue(), fieldDef.fieldType);

        // 6. check valid
        if (!fieldDef.isValid() && tableType != CUSTOM_TABLE_KHRONOS) {
            AUTIL_LOG(WARN, "field [%s] is not valid", fieldDef.fieldName.c_str());
            return false;
        }
        fields.emplace_back(fieldDef);
    }
    return true;
}

bool IndexPartitionSchemaConverter::isCustomizedFieldTypeToDoubleArray(
    const std::string &fieldType) {
    std::string newFieldType = fieldType;
    autil::StringUtil::toUpperCase(newFieldType);
    if (newFieldType == "LOCATION" || newFieldType == "LINE" || newFieldType == "POLYGON"
        || newFieldType == "SHAPE") {
        return true;
    }
    return false;
}

bool IndexPartitionSchemaConverter::isCustomizedFieldTypeToString(const std::string &fieldType) {
    std::string newFieldType = fieldType;
    autil::StringUtil::toUpperCase(newFieldType);
    if (newFieldType == "RAW") {
        return true;
    }
    return false;
}

bool IndexPartitionSchemaConverter::isNormalPkIndex(const std::string &indexType) {
    std::string newIndexType = indexType;
    autil::StringUtil::toUpperCase(newIndexType);
    if (newIndexType == PK64_INDEX_TYPE || newIndexType == PK128_INDEX_TYPE) {
        return true;
    }
    return false;
}

} // namespace sql
