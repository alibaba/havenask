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
#include "sql/resource/Ha3TableInfoR.h"

#include <algorithm>
#include <engine/NaviConfigContext.h>
#include <map>
#include <memory>
#include <utility>

#include "autil/legacy/legacy_jsonizable.h"
#include "ha3/common/CommonDef.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/partition/index_application.h"
#include "indexlib/table/BuiltinDefine.h"
#include "iquan/common/Common.h"
#include "iquan/common/catalog/TableModel.h"
#include "iquan/config/JniConfig.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/log/NaviLogger.h"
#include "navi/proto/KernelDef.pb.h"
#include "sql/common/IndexPartitionSchemaConverter.h"
#include "sql/resource/KhronosTableConverter.h"
#include "suez/turing/common/JoinConfigInfo.h"
#include "suez/turing/expression/util/TableInfo.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

using namespace indexlib::partition;
using namespace suez::turing;
using namespace iquan;

namespace sql {

const std::string Ha3TableInfoR::RESOURCE_ID = "sql.table_info_r";

Ha3TableInfoR::Ha3TableInfoR() {}

Ha3TableInfoR::~Ha3TableInfoR() {}

#define ADD_TABLE_AND_TABLEIDENTITY(catalogDefs, dbName, locationSign, tableModel)                 \
    do {                                                                                           \
        if (!catalogDefs.catalog(SQL_DEFAULT_CATALOG_NAME)                                         \
                 .database(dbName)                                                                 \
                 .addTable(tableModel)) {                                                          \
            NAVI_KERNEL_LOG(ERROR, "add table [%s] failed", tableModel.tableName().c_str());       \
            return false;                                                                          \
        }                                                                                          \
        TableIdentity tableIdentity(dbName, tableModel.tableName());                               \
        if (!catalogDefs.catalog(SQL_DEFAULT_CATALOG_NAME)                                         \
                 .location(locationSign)                                                           \
                 .addTableIdentity(tableIdentity)) {                                               \
            NAVI_KERNEL_LOG(ERROR,                                                                 \
                            "add table identity [%s] [%s] failed",                                 \
                            tableIdentity.dbName.c_str(),                                          \
                            tableIdentity.tableName.c_str());                                      \
            return false;                                                                          \
        }                                                                                          \
    } while (false)

void Ha3TableInfoR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_BIZ);
}

bool Ha3TableInfoR::config(navi::ResourceConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "zone_name", _zoneName);
    NAVI_JSONIZE(ctx, "part_count", _partCount);
    NAVI_JSONIZE(
        ctx, "inner_docid_optimize_enable", _innerDocIdOptimizeEnable, _innerDocIdOptimizeEnable);
    return true;
}

navi::ErrorCode Ha3TableInfoR::init(navi::ResourceInitContext &ctx) {
    std::vector<std::shared_ptr<indexlibv2::config::ITabletSchema>> tableSchemas;
    if (!getTableSchemas(tableSchemas)) {
        return navi::EC_ABORT;
    }
    if (!generateMeta(tableSchemas)) {
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

bool Ha3TableInfoR::getTableSchemas(
    std::vector<std::shared_ptr<indexlibv2::config::ITabletSchema>> &tableSchemas) {
    const auto &id2IndexAppMap = _tableInfoR->getIndexAppMap();
    if (id2IndexAppMap.empty()) {
        NAVI_KERNEL_LOG(ERROR, "empty index app map");
        return false;
    }
    auto indexAppPtr = id2IndexAppMap.begin()->second;
    indexAppPtr->GetTableSchemas(tableSchemas);
    return true;
}

bool Ha3TableInfoR::generateMeta(
    const std::vector<std::shared_ptr<indexlibv2::config::ITabletSchema>> &tableSchemas) {
    const auto &joinRelations = _tableInfoR->getJoinRelations();
    const std::string &dbName = _zoneName;
    iquan::CatalogDefs catalogDefs;
    LocationSign locationSign = {_partCount, _zoneName, "searcher"};
    catalogDefs.catalog(SQL_DEFAULT_CATALOG_NAME).location(locationSign);

    // merge IndexPartitionSchema from tablet
    for (const auto &tableSchema : tableSchemas) {
        iquan::TableDef tableDef;
        IndexPartitionSchemaConverter::convert(tableSchema, tableDef);
        if (_innerDocIdOptimizeEnable
            && tableSchema->GetTableType() == indexlib::table::TABLE_TYPE_NORMAL) {
            addInnerDocId(tableDef);
        }
        const auto &tableName = tableSchema->GetTableName();
        if (!_ha3ClusterDefR->fillTableDef(_zoneName, tableName, tableDef, _tableSortDescMap)) {
            NAVI_KERNEL_LOG(ERROR,
                            "fillTableDef from cluster def failed, zoneName [%s] tableName [%s]",
                            _zoneName.c_str(),
                            tableName.c_str());
            return false;
        }
        iquan::TableModel tableModel;
        tableModel.tableContent = tableDef;

        if (KhronosTableConverter::isKhronosTable(tableModel.tableContent)) {
            if (!KhronosTableConverter::fillKhronosTable(
                    tableModel, _zoneName, locationSign, catalogDefs)) {
                NAVI_KERNEL_LOG(
                    ERROR, "fill khronos table failed, table name [%s]", tableName.c_str());
                return false;
            }
        } else {
            TableModel summaryTable;
            if (fillSummaryTable(tableModel, summaryTable)) {
                ADD_TABLE_AND_TABLEIDENTITY(catalogDefs, dbName, locationSign, summaryTable);
            }
            ADD_TABLE_AND_TABLEIDENTITY(catalogDefs, dbName, locationSign, tableModel);
            if (!addJoinInfo(SQL_DEFAULT_CATALOG_NAME,
                             dbName,
                             locationSign,
                             tableName,
                             joinRelations,
                             catalogDefs)) {
                return false;
            }
        }
    }
    _metaStr = autil::legacy::FastToJsonString(catalogDefs, true);
    NAVI_KERNEL_LOG(INFO, "metaStr: %s", _metaStr.c_str());
    return true;
}

bool Ha3TableInfoR::fillSummaryTable(TableModel &tableModel, TableModel &summaryTable) {
    auto &summaryFields = tableModel.tableContent.summaryFields;
    if (summaryFields.empty()) {
        return false;
    }

    summaryTable = tableModel;
    auto &summaryTableDef = summaryTable.tableContent;
    summaryTableDef.tableName = summaryTable.tableName() + iquan::SUMMARY_TABLE_SUFFIX;
    summaryTableDef.properties[PROP_KEY_IS_SUMMARY] = IQUAN_TRUE;
    if (!summaryFields.empty()) {
        summaryFields.clear();
        summaryTableDef.tableType = IQUAN_TABLE_TYPE_SUMMARY;
        summaryTableDef.fields = summaryTableDef.summaryFields;
        summaryTableDef.summaryFields.clear();
    }
    NAVI_KERNEL_LOG(INFO, "add [%s] summary table.", summaryTableDef.tableName.c_str());
    return true;
}

void Ha3TableInfoR::addInnerDocId(iquan::TableDef &tableDef) {
    iquan::FieldDef field;
    field.fieldName = isearch::common::INNER_DOCID_FIELD_NAME;
    field.indexName = isearch::common::INNER_DOCID_FIELD_NAME;
    field.indexType = "PRIMARYKEY64";
    field.fieldType.typeName = "INTEGER";
    tableDef.fields.emplace_back(std::move(field));
}

bool Ha3TableInfoR::addJoinInfo(const std::string &catalogName,
                                const std::string &dbName,
                                const iquan::LocationSign &locationSign,
                                const std::string &tableName,
                                const indexlib::partition::JoinRelationMap &joinRelations,
                                CatalogDefs &catalogDefs) {
    auto it = joinRelations.find(tableName);
    if (joinRelations.end() == it) {
        return true;
    }
    const auto &joinFields = it->second;
    for (const auto &joinField : joinFields) {
        iquan::JoinInfoDef def;
        def.mainTable = TableIdentity(dbName, tableName);
        def.auxTable = TableIdentity(dbName, joinField.joinTableName);
        def.joinField = joinField.fieldName;
        if (!catalogDefs.catalog(catalogName).location(locationSign).addJoinInfo(def)) {
            NAVI_KERNEL_LOG(
                ERROR,
                "add join info failed, main table id [%s] aux table id [%s] join field [%s]",
                def.mainTableId().c_str(),
                def.auxTableId().c_str(),
                def.joinField.c_str());
            return false;
        }
    }
    return true;
}

bool Ha3TableInfoR::getMeta(std::string &meta) const {
    meta = _metaStr;
    return true;
}

REGISTER_RESOURCE(Ha3TableInfoR);

} // namespace sql
