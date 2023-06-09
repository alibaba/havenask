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
#include "navi/engine/Resource.h"
#include "suez/turing/common/JoinConfigInfo.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "suez_navi/resource/IndexProviderR.h"

namespace suez_navi {

class TableInfoR : public navi::Resource
{
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
    indexlib::partition::PartitionReaderSnapshotPtr createDefaultIndexSnapshot(
            const std::string &leadingTableName = "");
    indexlib::partition::PartitionReaderSnapshotPtr createPartitionReaderSnapshot(
            int32_t partid, const std::string &leadingTableName = "");
    const suez::turing::TableInfoPtr &getTableInfoWithRel() const;
    const std::map<std::string, suez::turing::TableInfoPtr> &getTableInfoMapWithoutRel() const;
public:
    static const std::string RESOURCE_ID;
private:
    IndexProviderR *_indexProviderR;
    std::string _itemTableName;
    std::vector<std::string> _dependTables;
    std::vector<suez::turing::JoinConfigInfo> _joinConfigInfos;

    int32_t _maxTablePartCount = -1;
    std::map<int32_t, indexlib::partition::IndexApplicationPtr> _id2IndexAppMap;
    suez::turing::TableInfoPtr _tableInfoWithRel;
    std::map<std::string, suez::turing::TableInfoPtr> _tableInfoMapWithoutRel;
};

NAVI_TYPEDEF_PTR(TableInfoR);

}

