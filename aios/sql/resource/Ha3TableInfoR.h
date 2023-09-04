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

#include <string>
#include <unordered_map>
#include <vector>

#include "indexlib/partition/join_cache/join_field.h"
#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "sql/resource/Ha3ClusterDefR.h"
#include "suez/turing/navi/TableInfoR.h"

namespace iquan {
class TableDef;
} // namespace iquan
namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace sql {

class Ha3TableInfoR : public navi::Resource {
public:
    Ha3TableInfoR();
    ~Ha3TableInfoR();
    Ha3TableInfoR(const Ha3TableInfoR &) = delete;
    Ha3TableInfoR &operator=(const Ha3TableInfoR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;
    bool getMeta(std::string &meta) const override;

public:
    const TableSortDescMap &getTableSortDescMap() const {
        return _tableSortDescMap;
    }

private:
    bool generateMeta();

public:
    static void addInnerDocId(iquan::TableDef &tableDef);

private:
    static void addJoinInfo(const std::string &tableName,
                            const indexlib::partition::JoinRelationMap &joinRelations,
                            iquan::TableDef &tableDef);

public:
    static const std::string RESOURCE_ID;

private:
    RESOURCE_DEPEND_DECLARE();

private:
    RESOURCE_DEPEND_ON(suez::turing::TableInfoR, _tableInfoR);
    RESOURCE_DEPEND_ON(Ha3ClusterDefR, _ha3ClusterDefR);
    std::unordered_map<std::string, std::vector<suez::SortDescription>> _tableSortDescMap;
    std::string _zoneName;
    bool _innerDocIdOptimizeEnable = false;
    std::string _metaStr;
};

NAVI_TYPEDEF_PTR(Ha3TableInfoR);

} // namespace sql
