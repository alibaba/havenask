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
#include "suez_navi/resource/TableInfoR.h"
#include "indexlib/partition/join_cache/join_field.h"
#include "suez/turing/search/base/MultiTableWrapper.h"

using namespace indexlib::partition;
using namespace suez::turing;

namespace suez_navi {

const std::string TableInfoR::RESOURCE_ID = "table_info_r";

TableInfoR::TableInfoR()
    : _indexProviderR(nullptr)
{
}

TableInfoR::~TableInfoR() {
}

void TableInfoR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID)
        .depend(IndexProviderR::RESOURCE_ID, true, BIND_RESOURCE_TO(_indexProviderR));
}

bool TableInfoR::config(navi::ResourceConfigContext &ctx) {
    ctx.Jsonize("item_table", _itemTableName, _itemTableName);
    ctx.Jsonize("depend_tables", _dependTables, _dependTables);
    ctx.Jsonize("join_infos", _joinConfigInfos, _joinConfigInfos);
    return true;
}

navi::ErrorCode TableInfoR::init(navi::ResourceInitContext &ctx) {
    std::vector<JoinField> joinFields;
    for (auto &joinConfigInfo : _joinConfigInfos) {
        joinFields.push_back(
            JoinField(joinConfigInfo.joinField, joinConfigInfo.joinCluster,
                      joinConfigInfo.useJoinCache, joinConfigInfo.strongJoin));
    }
    JoinRelationMap joinRelations;
    if (!_itemTableName.empty() && !joinFields.empty()) {
        joinRelations[_itemTableName] = joinFields;
    }
    MultiTableWrapper tableWrapper;
    if (!tableWrapper.init(_indexProviderR->getIndexProvider().multiTableReader,
                           _dependTables, joinRelations, _itemTableName))
    {
        NAVI_KERNEL_LOG(ERROR, "init MultiTableWrapper failed");
        return navi::EC_ABORT;
    }

    _maxTablePartCount = tableWrapper.getMaxTablePartCount();
    _id2IndexAppMap = tableWrapper.getId2IndexAppMap();
    _tableInfoWithRel = tableWrapper.getTableInfoWithRelation();
    _tableInfoMapWithoutRel = tableWrapper.getTableInfoMapWithoutRelation();
    return navi::EC_NONE;
}

PartitionReaderSnapshotPtr TableInfoR::createDefaultIndexSnapshot(const std::string &leadingTableName) {
    if (_id2IndexAppMap.size() > 0) {
        auto indexApp = _id2IndexAppMap.begin()->second;
        if (indexApp) {
            return indexApp->CreateSnapshot(leadingTableName);
        }
    }
    return PartitionReaderSnapshotPtr();
}

PartitionReaderSnapshotPtr TableInfoR::createPartitionReaderSnapshot(int32_t partid,
        const std::string &leadingTableName)
{
    auto iter = _id2IndexAppMap.find(partid);
    if (iter != _id2IndexAppMap.end()) {
        auto indexApp = iter->second;
        if (indexApp) {
            return indexApp->CreateSnapshot(leadingTableName);
        }
    }
    NAVI_KERNEL_LOG(WARN, "not find part id [%d] index snapshot, use default", partid);
    return createDefaultIndexSnapshot(leadingTableName);
}

const TableInfoPtr &TableInfoR::getTableInfoWithRel() const {
    return _tableInfoWithRel;
}

const std::map<std::string, TableInfoPtr> &TableInfoR::getTableInfoMapWithoutRel() const {
    return _tableInfoMapWithoutRel;
}

REGISTER_RESOURCE(TableInfoR);

}

