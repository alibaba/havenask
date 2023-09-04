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

#include <functional>
#include <map>
#include <memory>
#include <string>

#include "autil/Log.h"
#include "catalog/entity/DiffResult.h"
#include "catalog/entity/EntityBase.h"
#include "catalog/entity/Partition.h"
#include "catalog/entity/Type.h"
#include "catalog/entity/UpdateResult.h"
#include "catalog/util/StatusBuilder.h"

namespace catalog {

SPECIALIZE_TRAITS(Table, TableId);

class Table : public EntityBase<Table> {
public:
    Table() = default;
    ~Table() = default;
    Table(const Table &) = delete;
    Table &operator=(const Table &) = delete;

public:
    const auto &tableConfig() const { return _tableConfig; }
    const auto &tableStructure() const { return _tableStructure; }
    const auto &partitions() const { return _partitions; }
    const auto &tableName() const { return _id.tableName; }
    const auto &databaseName() const { return _id.databaseName; }

    void copyDetail(Table *other) const;
    Status fromProto(const proto::Table &proto);
    template <typename T>
    Status toProto(T *proto, proto::DetailLevel::Code detailLevel = proto::DetailLevel::DETAILED) const;
    Status compare(const Table *other, DiffResult &diffResult) const;
    void updateChildStatus(const proto::EntityStatus &target, UpdateResult &updateResult);
    void alignVersion(CatalogVersion version);

    Status create(const proto::Table &request);
    Status update(const proto::Table &request);
    Status drop();

    // ---------------- API for TableStructure ------------------
    Status updateTableStructure(const proto::TableStructure &request);
    Status addColumn(const proto::AddColumnRequest &request);
    Status updateColumn(const proto::UpdateColumnRequest &request);
    Status dropColumn(const proto::DropColumnRequest &request);
    Status createIndex(const proto::CreateIndexRequest &request);
    Status updateIndex(const proto::UpdateIndexRequest &request);
    Status dropIndex(const proto::DropIndexRequest &request);

    // ---------------- API for Partition ------------------
    Status createPartition(const proto::Partition &request);
    Status updatePartition(const proto::Partition &request);
    Status updatePartitionTableStructure(const proto::UpdatePartitionTableStructureRequest &request);
    Status dropPartition(const PartitionId &id);
    Status getPartition(const std::string &partitionName, Partition *&partition) const;
    bool hasPartition(const std::string &partitionName) const;
    std::vector<std::string> listPartition() const;

private:
    Status fromProtoOrCreate(const proto::Table &proto, bool create);
    Status addEntityCheck(const proto::Partition &proto);
    using PartitionExec = std::function<Status(Partition &)>;
    Status execute(const std::string &partitionName, PartitionExec exec);
    using TableStructureExec = std::function<Status(TableStructure &)>;
    Status execute(TableStructureExec exec);

private:
    proto::TableConfig _tableConfig;
    std::unique_ptr<TableStructure> _tableStructure;
    std::map<std::string, std::unique_ptr<Partition>> _partitions;
    AUTIL_LOG_DECLARE();
};

extern template Status Table::toProto<>(proto::Table *, proto::DetailLevel::Code) const;
extern template Status Table::toProto<>(proto::TableRecord *, proto::DetailLevel::Code) const;

} // namespace catalog
