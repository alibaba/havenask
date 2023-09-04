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

#include "indexlib/partition/index_application.h"
#include "indexlib/partition/join_cache/join_field.h"
#include "indexlib/partition/partition_reader_snapshot.h"
#include "navi/engine/Resource.h"
#include "suez/sdk/IndexProviderR.h"
#include "suez/turing/common/JoinConfigInfo.h"
#include "suez/turing/expression/util/TableInfo.h"

namespace suez {
namespace turing {

class TableInfoR : public navi::Resource {
public:
    TableInfoR();
    ~TableInfoR();
    TableInfoR(const TableInfoR &) = delete;
    TableInfoR &operator=(const TableInfoR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    indexlib::partition::PartitionReaderSnapshotPtr
    createDefaultIndexSnapshot(const std::string &leadingTableName = "");
    indexlib::partition::PartitionReaderSnapshotPtr
    createPartitionReaderSnapshot(navi::NaviPartId partId, const std::string &leadingTableName = "");
    const std::map<std::string, std::string> &getTablePathMap() const { return _tablePathMap; }
    const suez::turing::TableInfoPtr &getTableInfoWithRel() const;
    const std::map<std::string, suez::turing::TableInfoPtr> &getTableInfoMapWithoutRel() const;
    const std::map<int32_t, indexlib::partition::IndexApplicationPtr> &getIndexAppMap() const {
        return _id2IndexAppMap;
    }
    int32_t getMaxTablePartCount() const { return _maxTablePartCount; }
    const indexlib::partition::JoinRelationMap &getJoinRelations() const { return _joinRelations; }
    suez::turing::TableInfoPtr getTableInfo(const std::string &tableName) const;

private:
    void initTablePathMap();
    void prepareJoinRelation();

public:
    static const std::string RESOURCE_ID;

private:
    RESOURCE_DEPEND_DECLARE();

private:
    RESOURCE_DEPEND_ON(suez::IndexProviderR, _indexProviderR);
    std::string _itemTableName;
    std::vector<std::string> _dependTables;
    std::vector<suez::turing::JoinConfigInfo> _joinConfigInfos;
    bool _enableMultiPartition = false;

    int32_t _maxTablePartCount = -1;
    std::map<std::string, std::string> _tablePathMap;
    std::map<int32_t, indexlib::partition::IndexApplicationPtr> _id2IndexAppMap;
    suez::turing::TableInfoPtr _tableInfoWithRel;
    std::map<std::string, suez::turing::TableInfoPtr> _tableInfoMapWithoutRel;
    indexlib::partition::JoinRelationMap _joinRelations;
    int64_t _cacheSnapshotTime = 500 * 1000;
};

NAVI_TYPEDEF_PTR(TableInfoR);

} // namespace turing
} // namespace suez
