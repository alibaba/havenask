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
#include "suez/turing/navi/TableInfoR.h"

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/table/BuiltinDefine.h"
#include "iquan/common/catalog/TableModel.h"
#include "suez/turing/search/base/MultiTableWrapper.h"

using namespace indexlib::partition;

namespace suez {
namespace turing {

const std::string TableInfoR::RESOURCE_ID = "suez_turing.table_info_r";

TableInfoR::TableInfoR() {}

TableInfoR::~TableInfoR() {}

void TableInfoR::def(navi::ResourceDefBuilder &builder) const { builder.name(RESOURCE_ID, navi::RS_BIZ); }

bool TableInfoR::config(navi::ResourceConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "item_table", _itemTableName, _itemTableName);
    NAVI_JSONIZE(ctx, "depend_tables", _dependTables, _dependTables);
    NAVI_JSONIZE(ctx, "join_infos", _joinConfigInfos, _joinConfigInfos);
    NAVI_JSONIZE(ctx, "enable_multi_partition", _enableMultiPartition, _enableMultiPartition);
    NAVI_JSONIZE(ctx, "cache_partition_snapshot_time_us", _cacheSnapshotTime, _cacheSnapshotTime);
    return true;
}

navi::ErrorCode TableInfoR::init(navi::ResourceInitContext &ctx) {
    initTablePathMap();
    prepareJoinRelation();
    MultiTableWrapper tableWrapper;
    const auto &multiTableReader = _indexProviderR->getIndexProvider().multiTableReader;
    if (!tableWrapper.init(multiTableReader, _dependTables, _joinRelations, _itemTableName)) {
        NAVI_KERNEL_LOG(ERROR, "init MultiTableWrapper failed");
        return navi::EC_ABORT;
    }
    _maxTablePartCount = tableWrapper.getMaxTablePartCount();
    _id2IndexAppMap = tableWrapper.getId2IndexAppMap();
    _tableInfoWithRel = tableWrapper.getTableInfoWithRelation();
    _tableInfoMapWithoutRel = tableWrapper.getTableInfoMapWithoutRelation();
    if (!_enableMultiPartition && _id2IndexAppMap.size() > 1) {
        NAVI_KERNEL_LOG(
            ERROR, "multi partition load not allowed, current load part count [%lu]", _id2IndexAppMap.size());
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

void TableInfoR::prepareJoinRelation() {
    for (auto &joinConfigInfo : _joinConfigInfos) {
        const std::string &mainTableName = joinConfigInfo.mainTable.empty() ? _itemTableName : joinConfigInfo.mainTable;
        JoinField joinField(joinConfigInfo.joinField,
                            joinConfigInfo.joinCluster,
                            joinConfigInfo.useJoinCache,
                            joinConfigInfo.strongJoin);
        if (_joinRelations.find(mainTableName) == _joinRelations.end()) {
            _joinRelations[mainTableName] = {joinField};
        } else {
            _joinRelations[mainTableName].emplace_back(joinField);
        }
    }
    NAVI_KERNEL_LOG(INFO, "join relation is `%s`", autil::StringUtil::toString(_joinRelations).c_str());
}

void TableInfoR::initTablePathMap() {
    const auto &multiTableReader = _indexProviderR->getIndexProvider().multiTableReader;
    for (const auto &tableName : _dependTables) {
        auto singleTableReaderMap = multiTableReader.getTableReaders(tableName);
        if (singleTableReaderMap.empty()) {
            NAVI_KERNEL_LOG(WARN, "table %s not exist in multiTableReader.", tableName.c_str());
            continue;
        }
        if (singleTableReaderMap.size() == 0 || (!_enableMultiPartition && singleTableReaderMap.size() > 1)) {
            NAVI_KERNEL_LOG(WARN,
                            "table [%s] has [%lu] partition in multiTableReader.",
                            tableName.c_str(),
                            singleTableReaderMap.size());
        }
        auto singleTableReader = singleTableReaderMap.begin()->second;
        _tablePathMap[tableName] = singleTableReader->getFilePath();
    }
}

PartitionReaderSnapshotPtr TableInfoR::createDefaultIndexSnapshot(const std::string &leadingTableName) {
    if (_id2IndexAppMap.size() > 0) {
        auto indexApp = _id2IndexAppMap.begin()->second;
        if (indexApp) {
            return indexApp->CreateSnapshot(leadingTableName, _cacheSnapshotTime);
        }
    }
    return PartitionReaderSnapshotPtr();
}

PartitionReaderSnapshotPtr TableInfoR::createPartitionReaderSnapshot(navi::NaviPartId partId,
                                                                     const std::string &leadingTableName) {
    auto iter = _id2IndexAppMap.find(partId);
    if (iter != _id2IndexAppMap.end()) {
        auto indexApp = iter->second;
        if (indexApp) {
            return indexApp->CreateSnapshot(leadingTableName, _cacheSnapshotTime);
        }
    }
    NAVI_KERNEL_LOG(WARN, "not find part id [%d] index snapshot, use default", partId);
    return createDefaultIndexSnapshot(leadingTableName);
}

const TableInfoPtr &TableInfoR::getTableInfoWithRel() const { return _tableInfoWithRel; }

const std::map<std::string, TableInfoPtr> &TableInfoR::getTableInfoMapWithoutRel() const {
    return _tableInfoMapWithoutRel;
}

suez::turing::TableInfoPtr TableInfoR::getTableInfo(const std::string &tableName) const {
    const auto &dependencyTableInfoMap = getTableInfoMapWithoutRel();
    auto tableInfo = dependencyTableInfoMap.find(tableName);
    if (tableInfo != dependencyTableInfoMap.end()) {
        return tableInfo->second;
    }
    NAVI_KERNEL_LOG(WARN, "get table info failed, tableName [%s]", tableName.c_str());
    return suez::turing::TableInfoPtr();
}

REGISTER_RESOURCE(TableInfoR);

} // namespace turing
} // namespace suez
