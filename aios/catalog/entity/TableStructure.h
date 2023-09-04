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

#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "catalog/entity/DiffResult.h"
#include "catalog/entity/EntityBase.h"
#include "catalog/entity/Type.h"
#include "catalog/entity/UpdateResult.h"
#include "catalog/util/StatusBuilder.h"

namespace catalog {

SPECIALIZE_TRAITS(TableStructure, TableStructureId);

class TableStructure : public EntityBase<TableStructure> {
public:
    TableStructure() = default;
    ~TableStructure() = default;
    TableStructure(const TableStructure &) = delete;
    TableStructure &operator=(const TableStructure &) = delete;

public:
    const auto &columns() const { return _columns; }
    const auto &indexes() const { return _indexes; }
    const auto &tableStructureConfig() const { return _tableStructureConfig; }
    const auto &shardInfo() const { return _shardInfo; }
    auto tableType() const { return _tableType; }
    const auto &comment() const { return _comment; }
    auto buildType() const { return _tableStructureConfig.build_type(); }

    void copyDetail(TableStructure *other) const;
    Status fromProto(const proto::TableStructure &proto);
    Status toProto(proto::TableStructure *proto,
                   proto::DetailLevel::Code detailLevel = proto::DetailLevel::DETAILED) const;
    Status compare(const TableStructure *other, DiffResult &diffResult) const;
    void updateChildStatus(const proto::EntityStatus &target, UpdateResult &updateResult);
    void alignVersion(CatalogVersion version);

    Status create(const proto::TableStructure &request);
    Status update(const proto::TableStructure &request);
    Status drop();

    Status addColumn(const proto::TableStructure::Column &request);
    Status updateColumn(const proto::TableStructure::Column &request);
    Status dropColumn(const std::string &columnName);
    Status createIndex(const proto::TableStructure::Index &request);
    Status updateIndex(const proto::TableStructure::Index &request);
    Status dropIndex(const std::string &indexName);

    using ColumnIter = std::vector<proto::TableStructure::Column>::iterator;
    using IndexIter = std::vector<proto::TableStructure::Index>::iterator;

    ColumnIter getColumn(const std::string &name);
    IndexIter getIndex(const std::string &name);

private:
    Status fromProtoOrCreate(const proto::TableStructure &proto);

private:
    std::vector<proto::TableStructure::Column> _columns;
    std::vector<proto::TableStructure::Index> _indexes;
    proto::TableStructureConfig _tableStructureConfig;
    proto::TableStructureConfig::ShardInfo _shardInfo;
    proto::TableType::Code _tableType;
    std::string _comment;
    AUTIL_LOG_DECLARE();
};

} // namespace catalog
