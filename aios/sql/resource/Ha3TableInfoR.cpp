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
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/log/NaviLogger.h"
#include "navi/proto/KernelDef.pb.h"
#include "sql/common/IndexPartitionSchemaConverter.h"
#include "suez/turing/common/JoinConfigInfo.h"
#include "suez/turing/expression/util/TableInfo.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

using namespace indexlib::partition;
using namespace suez::turing;

namespace sql {

const std::string Ha3TableInfoR::RESOURCE_ID = "sql.table_info_r";

Ha3TableInfoR::Ha3TableInfoR() {}

Ha3TableInfoR::~Ha3TableInfoR() {}

void Ha3TableInfoR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_BIZ);
}

bool Ha3TableInfoR::config(navi::ResourceConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "zone_name", _zoneName);
    NAVI_JSONIZE(
        ctx, "inner_docid_optimize_enable", _innerDocIdOptimizeEnable, _innerDocIdOptimizeEnable);
    return true;
}

navi::ErrorCode Ha3TableInfoR::init(navi::ResourceInitContext &ctx) {
    if (!generateMeta()) {
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

bool Ha3TableInfoR::generateMeta() {
    const auto &joinRelations = _tableInfoR->getJoinRelations();
    const auto &id2IndexAppMap = _tableInfoR->getIndexAppMap();
    if (id2IndexAppMap.empty()) {
        NAVI_KERNEL_LOG(ERROR, "empty index app map");
        return false;
    }
    std::vector<std::shared_ptr<indexlibv2::config::ITabletSchema>> tableSchemas;
    auto indexAppPtr = id2IndexAppMap.begin()->second;
    indexAppPtr->GetTableSchemas(tableSchemas);

    const auto &tableInfoMapWithoutRel = _tableInfoR->getTableInfoMapWithoutRel();
    auto maxTablePartCount = _tableInfoR->getMaxTablePartCount();
    iquan::TableModels tableModels;
    // merge IndexPartitionSchema from tablet
    for (const auto &tableSchema : tableSchemas) {
        iquan::TableDef tableDef;
        IndexPartitionSchemaConverter::convert(tableSchema, tableDef);
        if (_innerDocIdOptimizeEnable
            && tableSchema->GetTableType() == indexlib::table::TABLE_TYPE_NORMAL) {
            addInnerDocId(tableDef);
        }
        const auto &tableName = tableSchema->GetTableName();
        if (!_ha3ClusterDefR->fillTableDef(
                _zoneName, tableName, maxTablePartCount, tableDef, _tableSortDescMap)) {
            NAVI_KERNEL_LOG(ERROR,
                            "fillTableDef from cluster def failed, zoneName [%s] tableName [%s]",
                            _zoneName.c_str(),
                            tableName.c_str());
            return false;
        }
        addJoinInfo(tableName, joinRelations, tableDef);
        iquan::TableModel tableModel;
        tableModel.catalogName = SQL_DEFAULT_CATALOG_NAME;
        tableModel.databaseName = _zoneName;
        tableModel.tableName = tableName;
        tableModel.tableType = tableDef.tableType;
        tableModel.tableContentVersion = "json_default_0.1";
        tableModel.tableContent = tableDef;
        const auto &tableInfo = tableInfoMapWithoutRel;
        const auto &it = tableInfo.find(tableName);
        if (it == tableInfo.end()) {
            NAVI_KERNEL_LOG(
                ERROR, "can not found table [%s] from index partition", tableName.c_str());
            return false;
        }
        tableModel.tableVersion = it->second->getTableVersion();
        tableModels.tables.emplace_back(std::move(tableModel));
    }
    _metaStr = autil::legacy::FastToJsonString(tableModels, true);
    NAVI_KERNEL_LOG(INFO, "metaStr: %s", _metaStr.c_str());
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

void Ha3TableInfoR::addJoinInfo(const std::string &tableName,
                                const indexlib::partition::JoinRelationMap &joinRelations,
                                iquan::TableDef &tableDef) {
    auto it = joinRelations.find(tableName);
    if (joinRelations.end() == it) {
        return;
    }
    const auto &joinFields = it->second;
    for (const auto &joinField : joinFields) {
        iquan::JoinInfoDef def;
        def.tableName = joinField.joinTableName;
        def.joinField = joinField.fieldName;
        tableDef.joinInfo.emplace_back(def);
    }
}

bool Ha3TableInfoR::getMeta(std::string &meta) const {
    meta = _metaStr;
    return true;
}

REGISTER_RESOURCE(Ha3TableInfoR);

} // namespace sql
