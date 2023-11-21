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
#include "sql/resource/KhronosTableConverter.h"

#include "iquan/common/Utils.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"

using namespace iquan;

namespace sql {

#define ADD_TABLE_AND_TABLEIDENTITY(catalogDefs, catalogName, dbName, locationSign, tableModel)    \
    if (!catalogDefs.catalog(catalogName).database(dbName).addTable(tableModel)) {                 \
        SQL_LOG(ERROR,                                                                             \
                "dup add table [%s] to catalog [%s] database [%s] failed",                         \
                tableModel.tableName().c_str(),                                                    \
                catalogName.c_str(),                                                               \
                dbName.c_str());                                                                   \
        return false;                                                                              \
    }                                                                                              \
    TableIdentity tableIdentity(dbName, tableModel.tableName());                                   \
    if (!catalogDefs.catalog(catalogName)                                                          \
             .location(locationSign)                                                               \
             .addTableIdentity(tableIdentity)) {                                                   \
        SQL_LOG(ERROR,                                                                             \
                "dup add table identity [%s] [%s] to catalog [%s] database [%s] failed",           \
                tableIdentity.dbName.c_str(),                                                      \
                tableIdentity.tableName.c_str(),                                                   \
                catalogName.c_str(),                                                               \
                dbName.c_str());                                                                   \
        return false;                                                                              \
    }

bool KhronosTableConverter::isKhronosTable(const TableDef &td) {
    if (td.tableType == IQUAN_TABLE_TYPE_KHRONOS) {
        return true;
    }
    if (td.tableType != IQUAN_TABLE_TYPE_CUSTOMIZED) {
        return false;
    }
    const auto &properties = td.properties;
    const auto &itIdentifier = properties.find(CUSTOM_IDENTIFIER);
    if (itIdentifier == properties.end() || itIdentifier->second != CUSTOM_TABLE_KHRONOS) {
        return false;
    }
    return true;
}

bool KhronosTableConverter::fillKhronosTable(const TableModel &tm,
                                             const std::string &dbName,
                                             const iquan::LocationSign &locationSign,
                                             CatalogDefs &catalogDefs) {
    if (!isKhronosTable(tm.tableContent)) {
        return false;
    }
    if (!fillKhronosMetaTable(tm, dbName, locationSign, catalogDefs)) {
        SQL_LOG(WARN, "fill khronos meta table failed, table[%s]", tm.tableName().c_str());
        return false;
    }
    if (!fillKhronosLogicTableV3(tm, dbName, locationSign, catalogDefs)) {
        SQL_LOG(WARN, "fill khronos series v3 table failed, table[%s]", tm.tableName().c_str());
        return false;
    }
    if (!fillKhronosLogicTableV2(tm, dbName, locationSign, catalogDefs)) {
        SQL_LOG(WARN, "fill khronos series table failed, table[%s]", tm.tableName().c_str());
        return false;
    }
    SQL_LOG(INFO, "add [%s] khronos table.", tm.tableName().c_str());
    return true;
}

bool KhronosTableConverter::fillKhronosMetaTable(const TableModel &tm,
                                                 const std::string &originDbName,
                                                 const LocationSign &locationSign,
                                                 CatalogDefs &catalogDefs) {
    const auto &tdName = tm.tableContent.tableName;
    // v2
    std::string tableName = KHRONOS_META_TABLE_NAME;
    std::string catalogName = tdName;
    if (!doFillKhronosMetaTable(
            tableName, originDbName, catalogName, tm, locationSign, catalogDefs)) {
        SQL_LOG(WARN, "fill khronos meta table failed, table[%s]", tableName.c_str());
        return false;
    }
    // v3
    tableName = tdName + KHRONOS_META_TABLE_NAME_V3_SUFFIX;
    catalogName = SQL_DEFAULT_CATALOG_NAME;
    if (!doFillKhronosMetaTable(
            tableName, originDbName, catalogName, tm, locationSign, catalogDefs)) {
        SQL_LOG(WARN, "fill khronos meta table v3 failed, table[%s]", tableName.c_str());
        return false;
    }
    return true;
}

bool KhronosTableConverter::doFillKhronosMetaTable(const std::string &tableName,
                                                   const std::string &dbName,
                                                   const std::string &catalogName,
                                                   const TableModel &tm,
                                                   const LocationSign &locationSign,
                                                   CatalogDefs &catalogDefs) {
    std::string tableType = IQUAN_TABLE_TYPE_KHRONOS_META;

    TableModel tableModel;

    TableDef tableDef;
    const auto &td = tm.tableContent;
    tableDef.distribution = td.distribution;

    tableDef.tableName = tableName;
    tableDef.tableType = tableType;

    FieldDef tsField;
    tsField.fieldName = KHRONOS_FIELD_NAME_TS;
    tsField.indexType = KHRONOS_INDEX_TYPE_TIMESTAMP;
    tsField.indexName = tsField.fieldName;
    tsField.fieldType.typeName = "INT32";
    tableDef.addFieldByMove(std::move(tsField));

    FieldDef metricField;
    metricField.fieldName = KHRONOS_FIELD_NAME_METRIC;
    metricField.indexType = KHRONOS_INDEX_TYPE_METRIC;
    metricField.indexName = metricField.fieldName;
    metricField.fieldType.typeName = "STRING";
    tableDef.addFieldByMove(std::move(metricField));

    FieldDef tagkField;
    tagkField.fieldName = KHRONOS_FIELD_NAME_TAGK;
    tagkField.indexType = KHRONOS_INDEX_TYPE_TAG_KEY;
    tagkField.indexName = tagkField.fieldName;
    tagkField.fieldType.typeName = "STRING";
    tableDef.addFieldByMove(std::move(tagkField));

    FieldDef tagvField;
    tagvField.fieldName = KHRONOS_FIELD_NAME_TAGV;
    tagvField.indexType = KHRONOS_INDEX_TYPE_TAG_VALUE;
    tagvField.indexName = tagvField.fieldName;
    tagvField.fieldType.typeName = "STRING";
    tableDef.addFieldByMove(std::move(tagvField));

    FieldDef fieldNameField;
    fieldNameField.fieldName = KHRONOS_FIELD_NAME_FIELD_NAME;
    fieldNameField.indexType = KHRONOS_INDEX_TYPE_FIELD_NAME;
    fieldNameField.indexName = fieldNameField.fieldName;
    fieldNameField.fieldType.typeName = "STRING";
    tableDef.fields.push_back(std::move(fieldNameField));

    tableModel.tableContent = tableDef;

    ADD_TABLE_AND_TABLEIDENTITY(catalogDefs, catalogName, dbName, locationSign, tableModel);
    return true;
}

bool KhronosTableConverter::fillKhronosDataFields(const std::string &khronosType,
                                                  const std::string &valueSuffix,
                                                  const std::string &fieldsStr,
                                                  const std::vector<FieldDef> fields,
                                                  TableDef &tableDef) {
    auto dataFields = autil::StringUtil::split(fieldsStr, "|");
    std::set<std::string> khronosDataFieldsSet;
    for (const auto &dataField : dataFields) {
        if (autil::legacy::EndWith(dataField, valueSuffix)) {
            khronosDataFieldsSet.insert(dataField);
        }
    }

    std::vector<FieldDef> tmpFields;
    for (const auto &fieldDef : fields) {
        if (1 == khronosDataFieldsSet.count(fieldDef.fieldName)) {
            if ("DOUBLE" != fieldDef.fieldType.typeName) {
                SQL_LOG(WARN, "field [%s] is not DOUBLE", fieldDef.fieldName.c_str());
                return false;
            }
            tmpFields.push_back(fieldDef);
        }
    }

    if (tmpFields.size() != khronosDataFieldsSet.size()) {
        SQL_LOG(WARN, "invalid field is found in dataFieldsStr[%s]", fieldsStr.c_str());
        return false;
    }

    for (const auto &field : tmpFields) {
        const auto &fieldName = field.fieldName;
        // add original *_value
        FieldDef fieldDef;
        fieldDef.fieldName = fieldName;
        fieldDef.fieldType.typeName = "STRING";
        fieldDef.indexType = KHRONOS_INDEX_TYPE_SERIES;
        fieldDef.indexName = fieldDef.fieldName;
        tableDef.addFieldByMove(std::move(fieldDef));
    }

    return true;
}

bool KhronosTableConverter::fillKhronosLogicTableV3(const TableModel &tm,
                                                    const std::string &dbName,
                                                    const LocationSign &locationSign,
                                                    CatalogDefs &catalogDefs) {
    const std::string khronosType = IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA_V3;
    const auto &td = tm.tableContent;
    TableModel tableModel;
    auto &tableDef = tableModel.tableContent;
    tableDef.distribution = td.distribution;
    tableDef.tableType = td.tableType;
    if (tableDef.tableType != SCAN_EXTERNAL_TABLE_TYPE) {
        tableDef.tableType = khronosType;
    }
    const std::string &catalogName = SQL_DEFAULT_CATALOG_NAME;
    tableDef.tableName = tm.tableName();
    // add metric field
    FieldDef metricField;
    metricField.fieldName = KHRONOS_FIELD_NAME_METRIC;
    metricField.indexType = KHRONOS_INDEX_TYPE_METRIC;
    metricField.indexName = metricField.fieldName;
    metricField.fieldType.typeName = "STRING";
    tableDef.fields.push_back(metricField);
    fillKhronosBuiltinFields(tableModel);
    ADD_TABLE_AND_TABLEIDENTITY(catalogDefs, catalogName, dbName, locationSign, tableModel);
    return true;
}

bool KhronosTableConverter::fillKhronosLogicTableV2(const TableModel &tm,
                                                    const std::string &dbName,
                                                    const LocationSign &locationSign,
                                                    CatalogDefs &catalogDefs) {
    const std::string khronosType = IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA;
    const auto &td = tm.tableContent;
    const auto &properties = td.properties;
    TableModel tableModel;

    auto &tableDef = tableModel.tableContent;
    std::string indexTableType = tm.tableType();
    tableDef.distribution = td.distribution;
    tableDef.tableType = khronosType;
    const std::string catalogName = td.tableName;

    tableDef.tableName = KHRONOS_DATA_TABLE_NAME_SERIES;
    if (indexTableType == IQUAN_TABLE_TYPE_CUSTOMIZED) {
        auto itFieldSuffix = properties.find(KHRONOS_PARAM_KEY_VALUE_FIELD_SUFFIX);
        if (itFieldSuffix == properties.end()) {
            SQL_LOG(ERROR, "khronos table [%s] not found", KHRONOS_PARAM_KEY_VALUE_FIELD_SUFFIX);
            return false;
        }
        const std::string &fieldSuffix = itFieldSuffix->second;
        auto itDataFields = properties.find(KHRONOS_PARAM_KEY_DATA_FIELDS);
        if (itDataFields == properties.end()) {
            SQL_LOG(ERROR, "khronos table [%s] not found", KHRONOS_PARAM_KEY_DATA_FIELDS);
            return false;
        }
        const std::string &dataFileds = itDataFields->second;
        if (!fillKhronosDataFields(khronosType, fieldSuffix, dataFileds, td.fields, tableDef)) {
            SQL_LOG(WARN, "fill khronos data fields failed");
            return false;
        }
    } else if (indexTableType == IQUAN_TABLE_TYPE_KHRONOS) {
        std::vector<std::string> fixedFieldNames
            = {"avg_value", "sum_value", "max_value", "min_value", "count_value"};
        for (const auto &fieldName : fixedFieldNames) {
            FieldDef fieldDef;
            fieldDef.fieldName = fieldName;
            fieldDef.fieldType.typeName = "STRING";
            fieldDef.indexType = KHRONOS_INDEX_TYPE_SERIES;
            fieldDef.indexName = fieldDef.fieldName;
            tableDef.fields.emplace_back(std::move(fieldDef));
        }
    }
    fillKhronosBuiltinFields(tableModel);
    ADD_TABLE_AND_TABLEIDENTITY(catalogDefs, catalogName, dbName, locationSign, tableModel);
    return true;
}

void KhronosTableConverter::fillKhronosBuiltinFields(TableModel &tableModel) {
    auto &tableDef = tableModel.tableContent;
    FieldDef tagsField;
    tagsField.fieldName = KHRONOS_FIELD_NAME_TAGS;
    tagsField.indexType = KHRONOS_INDEX_TYPE_TAGS;
    tagsField.indexName = tagsField.fieldName;
    tagsField.fieldType.typeName = "MAP";
    tagsField.fieldType.keyType["type"] = autil::legacy::ToJson("string");
    tagsField.fieldType.valueType["type"] = autil::legacy::ToJson("string");
    tableDef.fields.emplace_back(std::move(tagsField));

    FieldDef watermarkField;
    watermarkField.fieldName = KHRONOS_FIELD_NAME_WATERMARK;
    watermarkField.indexType = KHRONOS_INDEX_TYPE_WATERMARK;
    watermarkField.indexName = watermarkField.fieldName;
    watermarkField.fieldType.typeName = "INT64";
    tableDef.fields.push_back(watermarkField);

    // add series_value['xxx']
    FieldDef seriesValueField;
    seriesValueField.fieldType.typeName = "MAP";
    seriesValueField.fieldType.keyType["type"] = autil::legacy::ToJson("string");
    seriesValueField.fieldType.valueType["type"] = autil::legacy::ToJson("string");
    seriesValueField.fieldName = KHRONOS_FIELD_NAME_SERIES_VALUE;
    seriesValueField.indexType = KHRONOS_INDEX_TYPE_SERIES_VALUE;
    seriesValueField.indexName = KHRONOS_FIELD_NAME_SERIES_VALUE;
    tableDef.fields.emplace_back(std::move(seriesValueField));

    // add series_string['xxx']
    FieldDef seriesStringField;
    seriesStringField.fieldType.typeName = "MAP";
    seriesStringField.fieldType.keyType["type"] = autil::legacy::ToJson("string");
    seriesStringField.fieldType.valueType["type"] = autil::legacy::ToJson("string");
    seriesStringField.fieldName = KHRONOS_FIELD_NAME_SERIES_STRING;
    seriesStringField.indexType = KHRONOS_INDEX_TYPE_SERIES_STRING;
    seriesStringField.indexName = KHRONOS_FIELD_NAME_SERIES_STRING;
    tableDef.fields.emplace_back(std::move(seriesStringField));
}
} // namespace sql
