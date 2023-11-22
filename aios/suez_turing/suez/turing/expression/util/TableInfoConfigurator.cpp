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
#include "suez/turing/expression/util/TableInfoConfigurator.h"

#include <assert.h>
#include <cstddef>
#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "alog/Logger.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/customized_config.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "indexlib/table/BuiltinDefine.h"
#include "suez/turing/expression/util/AttributeInfos.h"
#include "suez/turing/expression/util/CustomizedTableInfo.h"
#include "suez/turing/expression/util/FieldInfos.h"
#include "suez/turing/expression/util/IndexInfos.h"
#include "suez/turing/expression/util/SummaryInfo.h"


using namespace std;
using namespace autil::legacy;
using namespace indexlib::config;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, TableInfoConfigurator);

TableInfoConfigurator::TableInfoConfigurator() {}

TableInfoConfigurator::~TableInfoConfigurator() {}

TableInfoPtr TableInfoConfigurator::createFromFile(const std::string &filePath) {
    string configStr;
    fslib::ErrorCode ec = fslib::fs::FileSystem::readFile(filePath, configStr);
    if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "Can't open file [%s] or the file is empty.", filePath.c_str());
        return TableInfoPtr();
    }
    return createFromString(configStr);
}

TableInfoPtr TableInfoConfigurator::createFromString(const std::string &configStr) {
    shared_ptr<indexlibv2::config::ITabletSchema> tabletSchema =
        indexlib::index_base::SchemaAdapter::LoadSchema(configStr);
    return createFromSchema(tabletSchema);
}

TableInfoPtr TableInfoConfigurator::createFromSchema(const IndexPartitionSchemaPtr &indexPartitionSchemaPtr,
                                                     int32_t version) {
    if (indexPartitionSchemaPtr == NULL) {
        return TableInfoPtr();
    }
    TableInfoPtr mainTableInfo = createTableInfo(indexPartitionSchemaPtr, version);
    if (mainTableInfo == NULL) {
        return TableInfoPtr();
    }
    IndexPartitionSchemaPtr subIndexPartitionSchema = indexPartitionSchemaPtr->GetSubIndexPartitionSchema();
    if (subIndexPartitionSchema != NULL) {
        TableInfoPtr subTableInfo = createTableInfo(subIndexPartitionSchema, version);
        if (subTableInfo != NULL) {
            mergeTableInfo(subTableInfo, mainTableInfo);
        }
    }
    return mainTableInfo;
}

TableInfoPtr TableInfoConfigurator::createFromSchema(const shared_ptr<indexlibv2::config::ITabletSchema> &schema,
                                                     int32_t version) {
    if (schema == NULL) {
        return TableInfoPtr();
    }

    const auto &legacySchemaAdapter = std::dynamic_pointer_cast<LegacySchemaAdapter>(schema);
    if (legacySchemaAdapter) {
        return createFromSchema(legacySchemaAdapter->GetLegacySchema());
    }
    return createTableInfo(schema, version);
}

TableInfoPtr TableInfoConfigurator::createFromIndexApp(const std::string &mainTable,
                                                       const indexlib::partition::IndexApplicationPtr &indexApp) {
    vector<shared_ptr<indexlibv2::config::ITabletSchema>> tableSchemas;
    indexApp->GetTableSchemas(tableSchemas);
    shared_ptr<indexlibv2::config::ITabletSchema> mainTableSchema;
    for (auto &schema : tableSchemas) {
        if (schema->GetTableName() == mainTable) {
            mainTableSchema = schema;
            break;
        }
    }
    if (!mainTableSchema) {
        AUTIL_LOG(WARN, "can not get mainTableSchema [%s]", mainTable.c_str());
        return TableInfoPtr();
    }
    auto tableInfo = createFromSchema(mainTableSchema);
    if (tableInfo == nullptr) {
        AUTIL_LOG(WARN, "table info is null, table [%s]", mainTable.c_str());
        return tableInfo;
    }
    AttributeInfos *attributeInfos = tableInfo->getAttributeInfos();
    FieldInfos *fieldInfos = tableInfo->getFieldInfos();
    if (!attributeInfos || !fieldInfos) {
        AUTIL_LOG(INFO, "main table [%s] has no attribute info and field info", mainTable.c_str());
        return tableInfo;
    }
    for (auto &schema : tableSchemas) {
        if (schema->GetTableName() == mainTable) {
            continue;
        }
        auto auxTableInfo = createFromSchema(schema);
        if (!auxTableInfo) {
            continue;
        }
        // TODO support pack
        auto joinAttributeInfos = auxTableInfo->getAttributeInfos();
        if (!joinAttributeInfos) {
            continue;
        }
        const auto &attributesVect = joinAttributeInfos->getAttrbuteInfoVector();
        for (size_t i = 0; i < attributesVect.size(); i++) {
            const AttributeInfo *attributeInfo = attributesVect[i];
            AttributeInfo *attrInfo(new AttributeInfo(*attributeInfo));
            attributeInfos->addAttributeInfo(attrInfo);
            const FieldInfo &fieldInfo = attributeInfo->getFieldInfo();
            FieldInfo *newFieldInfo(new FieldInfo(fieldInfo));
            fieldInfos->addFieldInfo(newFieldInfo);
        }
    }
    return tableInfo;
}

TableInfoPtr TableInfoConfigurator::createTableInfo(const IndexPartitionSchemaPtr &indexPartitionSchemaPtr,
                                                    int32_t version) {
    TableInfoPtr tableInfoPtr(new TableInfo());
    tableInfoPtr->setTableName(indexPartitionSchemaPtr->GetSchemaName());
    tableInfoPtr->setTableType(indexPartitionSchemaPtr->GetTableType());
    tableInfoPtr->setTableVersion(version);
    FieldSchemaPtr fieldSchemaPtr = indexPartitionSchemaPtr->GetFieldSchema();
    assert(fieldSchemaPtr);
    vector<shared_ptr<indexlibv2::config::FieldConfig>> fieldConfigs;
    for (auto it = fieldSchemaPtr->Begin(); it != fieldSchemaPtr->End(); ++it) {
        fieldConfigs.emplace_back(*it);
    }
    FieldInfos *fieldInfos = transToFieldInfos(fieldConfigs);
    if (fieldInfos) {
        tableInfoPtr->setFieldInfos(fieldInfos);
    }
    IndexSchemaPtr indexSchemaPtr = indexPartitionSchemaPtr->GetIndexSchema();
    if (indexSchemaPtr == NULL) {
        AUTIL_LOG(WARN, "IndexSchema is NULL");
        return TableInfoPtr();
    }

    auto indexInfos = transToIndexInfos(indexSchemaPtr);
    tableInfoPtr->setIndexInfos(indexInfos.release());
    tableInfoPtr->setPrimaryKeyAttributeFlag(indexSchemaPtr->HasPrimaryKeyAttribute());

    AttributeSchemaPtr attributeSchemaPtr = indexPartitionSchemaPtr->GetAttributeSchema();
    auto attributeInfos = transToAttributeInfos(fieldInfos, attributeSchemaPtr);
    tableInfoPtr->setAttributeInfos(attributeInfos.release());

    SummarySchemaPtr summarySchemaPtr = indexPartitionSchemaPtr->GetSummarySchema();
    auto summaryInfo = transToSummaryInfo(summarySchemaPtr);
    tableInfoPtr->setSummaryInfo(summaryInfo.release());

    CustomizedTableConfigPtr customizedTableConfigPtr = indexPartitionSchemaPtr->GetCustomizedTableConfig();
    auto customizedTableInfo = transToCustomizedTableInfo(customizedTableConfigPtr);
    tableInfoPtr->setCustomizedTableInfo(customizedTableInfo.release());
    return tableInfoPtr;
}

TableInfoPtr TableInfoConfigurator::createTableInfo(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema,
                                                    int32_t version) {
    TableInfoPtr tableInfoPtr(new TableInfo());
    tableInfoPtr->setTableName(schema->GetTableName());
    indexlib::TableType tableType;
    if (schema->GetTableType() == "aggregation") {
        tableType = indexlib::tt_kv;
    } else if (isKhronosTableType(schema)) {
        tableInfoPtr->setTableType(indexlib::tt_customized);
        tableInfoPtr->setTableVersion(version);
        return tableInfoPtr;
    } else if (!IndexPartitionSchema::Str2TableType(schema->GetTableType(), tableType)) {
        AUTIL_LOG(ERROR, "transfer table type fail, table name [%s]", schema->GetTableName().c_str());
        return TableInfoPtr();
    }
    tableInfoPtr->setTableType(tableType);
    tableInfoPtr->setTableVersion(version);
    const auto &fieldConfigs = schema->GetFieldConfigs();
    FieldInfos *fieldInfos = transToFieldInfos(fieldConfigs);
    if (fieldInfos) {
        tableInfoPtr->setFieldInfos(fieldInfos);
    }

    auto indexInfos = transToIndexInfos(schema);
    if (!indexInfos || indexInfos->getIndexCount() == 0) {
        AUTIL_LOG(WARN, "indexInfos [%p] is empty or index count is 0", indexInfos.get());
        return TableInfoPtr();
    }
    tableInfoPtr->setIndexInfos(indexInfos.release());
    bool hasPkAttr = false;
    const auto &pkConfig =
        std::dynamic_pointer_cast<indexlibv2::index::PrimaryKeyIndexConfig>(schema->GetPrimaryKeyIndexConfig());
    if (pkConfig) {
        hasPkAttr = pkConfig->HasPrimaryKeyAttribute();
    }
    tableInfoPtr->setPrimaryKeyAttributeFlag(hasPkAttr);

    auto attributeInfos = transToAttributeInfos(fieldInfos, schema);
    tableInfoPtr->setAttributeInfos(attributeInfos.release());

    auto summaryInfo = transToSummaryInfo(schema);
    tableInfoPtr->setSummaryInfo(summaryInfo.release());

    return tableInfoPtr;
}

FieldInfos *TableInfoConfigurator::transToFieldInfos(
    const std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> &fieldConfigs) {
    FieldInfos *fieldInfos = new FieldInfos;
    for (const auto &fieldConfig : fieldConfigs) {
        fieldInfos->addFieldInfo(transToFieldInfo(fieldConfig));
    }
    return fieldInfos;
}

FieldInfo *TableInfoConfigurator::transToFieldInfo(const shared_ptr<indexlibv2::config::FieldConfig> &fieldConfigPtr) {
    FieldInfo *fieldInfo = new FieldInfo;
    fieldInfo->fieldName = fieldConfigPtr->GetFieldName();
    fieldInfo->fieldType = fieldConfigPtr->GetFieldType();
    fieldInfo->isMultiValue = fieldConfigPtr->IsMultiValue();
    fieldInfo->analyzerName = fieldConfigPtr->GetAnalyzerName();
    return fieldInfo;
}

unique_ptr<IndexInfos>
TableInfoConfigurator::transToIndexInfos(const shared_ptr<indexlibv2::config::ITabletSchema> &schema) {
    auto createPkIndexInfo = [](const auto &config) {
        assert(config);
        const auto &singleFieldIndexConfig =
            std::dynamic_pointer_cast<indexlibv2::config::SingleFieldIndexConfig>(config);
        assert(singleFieldIndexConfig);
        IndexInfo *indexInfo = new IndexInfo;
        indexInfo->indexId = singleFieldIndexConfig->GetIndexId();
        indexInfo->indexName = singleFieldIndexConfig->GetIndexName();
        InvertedIndexType indexType = singleFieldIndexConfig->GetInvertedIndexType();
        indexInfo->indexType = indexType;
        indexInfo->isFieldIndex = true;
        const auto &fieldConfig = singleFieldIndexConfig->GetFieldConfig();
        indexInfo->fieldName = fieldConfig->GetFieldName();
        indexInfo->setSingleFieldType(fieldConfig->GetFieldType());
        if (it_primarykey64 == indexType || it_primarykey128 == indexType) {
            const auto &pkConfig =
                std::dynamic_pointer_cast<indexlibv2::index::PrimaryKeyIndexConfig>(singleFieldIndexConfig);
            if (pkConfig) {
                indexInfo->setPrimaryKeyHashType(pkConfig->GetPrimaryKeyHashType());
            }
        }
        indexInfo->setAnalyzerName(singleFieldIndexConfig->GetAnalyzer().c_str());
        return indexInfo;
    };
    auto indexInfos = make_unique<IndexInfos>();
    const auto &tableType = schema->GetTableType();
    if (tableType == indexlib::table::TABLE_TYPE_NORMAL) {
        const auto &invertedConfigs = schema->GetIndexConfigs(indexlib::index::GENERAL_INVERTED_INDEX_TYPE_STR);
        for (const auto &indexConfig : invertedConfigs) {
            const auto &invertedIndexConfig =
                std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
            assert(invertedIndexConfig);
            InvertedIndexType indexType = invertedIndexConfig->GetInvertedIndexType();
            if (indexType == it_primarykey128 || indexType == it_primarykey64) {
                continue;
            }
            IndexInfo *indexInfo = new IndexInfo;
            indexInfo->indexId = invertedIndexConfig->GetIndexId();
            indexInfo->indexName = invertedIndexConfig->GetIndexName();
            indexInfo->indexType = indexType;
            if (it_pack == indexType || it_expack == indexType || it_customized == indexType) {
                indexInfo->isFieldIndex = false;
                auto packageIndexConfigPtr =
                    std::dynamic_pointer_cast<indexlibv2::config::PackageIndexConfig>(invertedIndexConfig);
                assert(packageIndexConfigPtr);
                const auto &fieldConfigVector = packageIndexConfigPtr->GetFieldConfigVector();
                for (const auto &fieldConfig : fieldConfigVector) {
                    int32_t fieldBoost = packageIndexConfigPtr->GetFieldBoost(fieldConfig->GetFieldId());
                    indexInfo->addField(fieldConfig->GetFieldName().c_str(), (uint32_t)fieldBoost);
                }
            } else {
                indexInfo->isFieldIndex = true;
                auto singleIndexConfigPtr =
                    std::dynamic_pointer_cast<indexlibv2::config::SingleFieldIndexConfig>(invertedIndexConfig);
                auto fieldConfigPtr = singleIndexConfigPtr->GetFieldConfig();
                indexInfo->fieldName = fieldConfigPtr->GetFieldName();
                indexInfo->setSingleFieldType(fieldConfigPtr->GetFieldType());
            }
            indexInfo->setAnalyzerName(invertedIndexConfig->GetAnalyzer().c_str());
            indexInfos->addIndexInfo(indexInfo);
        }
        const auto &config = schema->GetPrimaryKeyIndexConfig();
        if (config) {
            indexInfos->addIndexInfo(createPkIndexInfo(config));
        }
    } else if (tableType == indexlib::table::TABLE_TYPE_KV || tableType == indexlib::table::TABLE_TYPE_KKV ||
               tableType == "aggregation") {
        const auto &config = schema->GetPrimaryKeyIndexConfig();
        assert(config);
        const auto &kvConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(config);
        assert(kvConfig);
        IndexInfo *indexInfo = new IndexInfo;
        indexInfo->indexId = 0;
        indexInfo->indexName = kvConfig->GetIndexName();
        indexInfo->indexType = it_kv;
        if (tableType == indexlib::table::TABLE_TYPE_KKV) {
            indexInfo->indexType = it_kkv;
        }
        indexInfo->isFieldIndex = true;
        const auto &fieldConfig = kvConfig->GetFieldConfig();
        indexInfo->fieldName = fieldConfig->GetFieldName();
        indexInfo->setSingleFieldType(fieldConfig->GetFieldType());
        indexInfos->addIndexInfo(indexInfo);
    } else if (tableType == indexlib::table::TABLE_TYPE_ORC) {
        const auto &config = schema->GetPrimaryKeyIndexConfig();
        indexInfos->addIndexInfo(createPkIndexInfo(config));
    } else {
        AUTIL_LOG(ERROR, "indexlibv2 does not support table type [%s] yet", tableType.c_str());
        assert(0);
        return nullptr;
    }
    return indexInfos;
}

unique_ptr<IndexInfos> TableInfoConfigurator::transToIndexInfos(IndexSchemaPtr indexSchemaPtr) {
    assert(indexSchemaPtr != NULL);
    auto indexInfos = make_unique<IndexInfos>();
    for (IndexSchema::Iterator it = indexSchemaPtr->Begin(); it != indexSchemaPtr->End(); ++it) {
        auto indexInfo = transToIndexInfo(*it);
        indexInfos->addIndexInfo(indexInfo.release());
    }
    return indexInfos;
}

unique_ptr<IndexInfo> TableInfoConfigurator::transToIndexInfo(IndexConfigPtr indexConfigPtr) {
    auto indexInfo = make_unique<IndexInfo>();
    indexInfo->indexId = indexConfigPtr->GetIndexId();
    indexInfo->indexName = indexConfigPtr->GetIndexName();
    InvertedIndexType indexType = indexConfigPtr->GetInvertedIndexType();
    indexInfo->indexType = indexType;
    if (it_pack == indexType || it_expack == indexType || it_customized == indexType) {
        indexInfo->isFieldIndex = false;
        PackageIndexConfigPtr packageIndexConfigPtr = DYNAMIC_POINTER_CAST(PackageIndexConfig, indexConfigPtr);
        const FieldConfigVector &fieldConfigVector = packageIndexConfigPtr->GetFieldConfigVector();
        for (FieldConfigVector::const_iterator it = fieldConfigVector.begin(); it != fieldConfigVector.end(); ++it) {
            int32_t fieldBoost = packageIndexConfigPtr->GetFieldBoost((*it)->GetFieldId());
            indexInfo->addField((*it)->GetFieldName().c_str(), (uint32_t)fieldBoost);
        }
    } else {
        indexInfo->isFieldIndex = true;
        SingleFieldIndexConfigPtr singleIndexConfigPtr = DYNAMIC_POINTER_CAST(SingleFieldIndexConfig, indexConfigPtr);
        FieldConfigPtr fieldConfigPtr = singleIndexConfigPtr->GetFieldConfig();
        indexInfo->fieldName = fieldConfigPtr->GetFieldName();
        indexInfo->setSingleFieldType(fieldConfigPtr->GetFieldType());
        if (it_primarykey64 == indexType || it_primarykey128 == indexType) {
            PrimaryKeyIndexConfigPtr pkIndexConfigPtr =
                DYNAMIC_POINTER_CAST(PrimaryKeyIndexConfig, singleIndexConfigPtr);
            if (pkIndexConfigPtr) {
                indexInfo->setPrimaryKeyHashType(pkIndexConfigPtr->GetPrimaryKeyHashType());
            }
        }
    }
    indexInfo->setAnalyzerName(indexConfigPtr->GetAnalyzer().c_str());
    return indexInfo;
}

unique_ptr<AttributeInfos> TableInfoConfigurator::transToAttributeInfos(FieldInfos *fieldInfos,
                                                                        AttributeSchemaPtr attributeSchemaPtr) {

    auto attributeInfos = make_unique<AttributeInfos>();
    if (NULL == attributeSchemaPtr) {
        return attributeInfos;
    }
    for (AttributeSchema::Iterator it = attributeSchemaPtr->Begin(); it != attributeSchemaPtr->End(); ++it) {
        AUTIL_LOG(DEBUG, "Find Attribute");
        AttributeInfo *attributeInfo = new AttributeInfo;
        FieldConfigPtr fieldConfigPtr = (*it)->GetFieldConfig();
        const string &fieldName = fieldConfigPtr->GetFieldName();
        const FieldInfo *fieldInfo = fieldInfos->getFieldInfo(fieldName.c_str());
        attributeInfo->setFieldInfo(*fieldInfo);
        attributeInfo->setAttrName(fieldInfo->fieldName);
        attributeInfo->setMultiValueFlag(fieldInfo->isMultiValue);
        attributeInfos->addAttributeInfo(attributeInfo);
    }
    return attributeInfos;
}

unique_ptr<AttributeInfos>
TableInfoConfigurator::transToAttributeInfos(FieldInfos *fieldInfos,
                                             const shared_ptr<indexlibv2::config::ITabletSchema> &schema) {
    auto createAttributeInfo = [fieldInfos](const std::shared_ptr<indexlibv2::config::FieldConfig> &fieldConfigPtr) {
        AUTIL_LOG(DEBUG, "Find Attribute");
        const string &fieldName = fieldConfigPtr->GetFieldName();
        const FieldInfo *fieldInfo = fieldInfos->getFieldInfo(fieldName.c_str());
        AttributeInfo *attributeInfo = new AttributeInfo;
        attributeInfo->setFieldInfo(*fieldInfo);
        attributeInfo->setAttrName(fieldInfo->fieldName);
        attributeInfo->setMultiValueFlag(fieldInfo->isMultiValue);
        return attributeInfo;
    };
    auto attributeInfos = make_unique<AttributeInfos>();
    const auto &fieldConfigs = schema->GetIndexFieldConfigs(indexlib::index::GENERAL_VALUE_INDEX_TYPE_STR);
    for (const auto &fieldConfig : fieldConfigs) {
        attributeInfos->addAttributeInfo(createAttributeInfo(fieldConfig));
    }
    return attributeInfos;
}

unique_ptr<SummaryInfo> TableInfoConfigurator::transToSummaryInfo(SummarySchemaPtr summarySchemaPtr) {
    auto summaryInfo = make_unique<SummaryInfo>();
    if (summarySchemaPtr == NULL) {
        return summaryInfo;
    }
    for (SummarySchema::Iterator it = summarySchemaPtr->Begin(); it != summarySchemaPtr->End(); ++it) {
        string fieldName = (*it)->GetSummaryName();
        summaryInfo->addFieldName(fieldName);
    }
    return summaryInfo;
}

unique_ptr<SummaryInfo>
TableInfoConfigurator::transToSummaryInfo(const shared_ptr<indexlibv2::config::ITabletSchema> &schema) {
    auto summaryInfo = make_unique<SummaryInfo>();
    const auto &indexConfigs = schema->GetIndexConfigs(indexlibv2::index::SUMMARY_INDEX_TYPE_STR);
    if (indexConfigs.empty()) {
        return summaryInfo;
    }
    assert(indexConfigs.size() == 1u);
    const auto &summaryIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::SummaryIndexConfig>(indexConfigs[0]);
    assert(summaryIndexConfig);
    for (const auto &summaryConfig : *summaryIndexConfig) {
        const string &fieldName = summaryConfig->GetSummaryName();
        summaryInfo->addFieldName(fieldName);
    }
    return summaryInfo;
}

unique_ptr<CustomizedTableInfo>
TableInfoConfigurator::transToCustomizedTableInfo(CustomizedTableConfigPtr customizedTableConfigPtr) {
    if (customizedTableConfigPtr == NULL) {
        return nullptr;
    }

    auto customizedTableInfo = make_unique<CustomizedTableInfo>();
    customizedTableInfo->setPluginName(customizedTableConfigPtr->GetPluginName());
    customizedTableInfo->setIdentifier(customizedTableConfigPtr->GetIdentifier());
    customizedTableInfo->setParameters(customizedTableConfigPtr->GetParameters());
    return customizedTableInfo;
}

void TableInfoConfigurator::mergeTableInfo(TableInfoPtr &subTableInfo, TableInfoPtr &mainTableInfo) {
    assert(subTableInfo && mainTableInfo);
    mergeFieldInfos(subTableInfo->getFieldInfos(), mainTableInfo->getFieldInfos());
    mergeAttributeInfos(subTableInfo->getAttributeInfos(), mainTableInfo->getAttributeInfos());
    mergeIndexInfos(subTableInfo->getIndexInfos(), mainTableInfo->getIndexInfos());
    mergeSummaryInfo(subTableInfo->getSummaryInfo(), mainTableInfo->getSummaryInfo());
    mainTableInfo->setSubSchemaFlag(true);
    mainTableInfo->setSubTableName(subTableInfo->getTableName());
}

void TableInfoConfigurator::mergeFieldInfos(FieldInfos *subFieldInfos, FieldInfos *mainFieldInfos) {
    assert(mainFieldInfos && subFieldInfos);
    vector<FieldInfo *> fieldInfos;
    subFieldInfos->stealFieldInfos(fieldInfos);
    for (size_t i = 0; i < fieldInfos.size(); i++) {
        mainFieldInfos->addFieldInfo(fieldInfos[i]);
    }
}

void TableInfoConfigurator::mergeAttributeInfos(AttributeInfos *subAttributeInfos, AttributeInfos *mainAttributeInfos) {
    assert(mainAttributeInfos && subAttributeInfos);
    AttributeInfos::AttrVector attrVec;
    subAttributeInfos->stealAttributeInfos(attrVec);
    for (size_t i = 0; i < attrVec.size(); i++) {
        attrVec[i]->setSubDocAttributeFlag(true);
        mainAttributeInfos->addAttributeInfo(attrVec[i]);
    }
}

void TableInfoConfigurator::mergeIndexInfos(IndexInfos *subIndexInfos, IndexInfos *mainIndexInfos) {
    assert(mainIndexInfos && subIndexInfos);
    IndexInfos::IndexVector indexInfos;
    subIndexInfos->stealIndexInfos(indexInfos);
    for (size_t i = 0; i < indexInfos.size(); i++) {
        indexInfos[i]->isSubDocIndex = true;
        mainIndexInfos->addIndexInfo(indexInfos[i]);
    }
}

void TableInfoConfigurator::mergeSummaryInfo(SummaryInfo *subSummaryInfo, SummaryInfo *mainSummaryInfo) {
    assert(mainSummaryInfo && subSummaryInfo);
    SummaryInfo::StringVector summaryInfo;
    subSummaryInfo->stealSummaryInfos(summaryInfo);
    for (size_t i = 0; i < summaryInfo.size(); i++) {
        mainSummaryInfo->addFieldName(summaryInfo[i]);
    }
}

bool TableInfoConfigurator::isKhronosTableType(const std::shared_ptr<indexlibv2::config::ITabletSchema> &tabletSchema) {
    auto legacySchema = tabletSchema->GetLegacySchema();
    if (legacySchema == nullptr) {
        return tabletSchema->GetTableType() == "khronos";
    }
    if (legacySchema->GetTableType() != indexlib::tt_customized) {
        return false;
    }
    const auto &customTableConfig = legacySchema->GetCustomizedTableConfig();
    if (customTableConfig != nullptr && customTableConfig->GetIdentifier() == "khronos") {
        return true;
    }
    return false;
}

} // namespace turing
} // namespace suez
