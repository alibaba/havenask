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
#include "catalog/entity/Function.h"
#include "catalog/entity/Table.h"
#include "catalog/entity/TableGroup.h"
#include "catalog/entity/Type.h"
#include "catalog/entity/UpdateResult.h"
#include "catalog/util/StatusBuilder.h"

namespace catalog {

SPECIALIZE_TRAITS(Database, DatabaseId);

class Database : public EntityBase<Database> {
public:
    Database() = default;
    ~Database() = default;
    Database(const Database &) = delete;
    Database &operator=(const Database &) = delete;

public:
    const auto &databaseConfig() const { return _databaseConfig; }
    const auto &tables() const { return _tables; };
    const auto &tableGroups() const { return _tableGroups; };
    const auto &functions() const { return _functions; };

    void copyDetail(Database *other) const;
    Status fromProto(const proto::Database &proto);
    template <typename T>
    Status toProto(T *proto, proto::DetailLevel::Code detailLevel = proto::DetailLevel::DETAILED) const;
    Status compare(const Database *other, DiffResult &diffResult) const;
    void updateChildStatus(const proto::EntityStatus &target, UpdateResult &result);
    void alignVersion(CatalogVersion version);

    Status create(const proto::Database &request);
    Status update(const proto::Database &request);
    Status drop();

    // ---------------- API for Table ----------------------
    Status createTable(const proto::Table &request);
    Status dropTable(const TableId &id);
    Status updateTable(const proto::Table &request);
    Status getTable(const std::string &tableName, Table *&table) const;
    bool hasTable(const std::string &tableName) const { return _tables.find(tableName) != _tables.end(); }
    std::vector<std::string> listTable() const;

    // ---------------- API for TableStructure -------------
    Status updateTableStructure(const proto::TableStructure &request);
    Status addColumn(const proto::AddColumnRequest &request);
    Status updateColumn(const proto::UpdateColumnRequest &request);
    Status dropColumn(const proto::DropColumnRequest &request);
    Status createIndex(const proto::CreateIndexRequest &request);
    Status updateIndex(const proto::UpdateIndexRequest &request);
    Status dropIndex(const proto::DropIndexRequest &request);

    // ---------------- API for Partition ------------------
    Status createPartition(const proto::CreatePartitionRequest &request);
    Status updatePartition(const proto::UpdatePartitionRequest &request);
    Status updatePartitionTableStructure(const proto::UpdatePartitionTableStructureRequest &request);
    Status dropPartition(const proto::DropPartitionRequest &request);

    // ---------------- API for TableGroup -----------------
    Status createTableGroup(const proto::TableGroup &request);
    Status updateTableGroup(const proto::TableGroup &request);
    Status dropTableGroup(const TableGroupId &id);
    Status getTableGroup(const std::string &tableGroupName, TableGroup *&tableGroup) const;
    bool hasTableGroup(const std::string &tableGroupName) const {
        return _tableGroups.find(tableGroupName) != _tableGroups.end();
    }
    std::vector<std::string> listTableGroup() const;

    Status createLoadStrategy(const proto::LoadStrategy &request);
    Status updateLoadStrategy(const proto::LoadStrategy &request);
    Status dropLoadStrategy(const LoadStrategyId &id);

    // ---------------- API for Function -------------------
    Status createFunction(const proto::Function &request);
    Status updateFunction(const proto::Function &request);
    Status dropFunction(const FunctionId &id);
    Status getFunction(const std::string &functionName, Function *&function) const;
    bool hasFunction(const std::string &functionName) const {
        return _functions.find(functionName) != _functions.end();
    }
    std::vector<std::string> listFunction() const;

    // ---------------- API for TargetGenerator ----------------
    Status getTableGroupTables(const std::string &tableGroupName, std::vector<const Table *> &tables);

private:
    Status fromProtoOrCreate(const proto::Database &proto, bool create);
    Status addEntityCheck(const proto::Table &proto);
    Status addEntityCheck(const proto::LoadStrategy &proto);
    Status addEntityCheck(const proto::TableGroup &proto);
    Status addEntityCheck(const proto::Function &proto);
    using TableExecute = std::function<Status(Table &)>;
    using TableGroupExecute = std::function<Status(TableGroup &)>;
    using FunctionExecute = std::function<Status(Function &)>;
    Status execute(const std::string &tableName, TableExecute exec);
    Status execute(const std::string &tableGroupName, TableGroupExecute exec);
    Status execute(const std::string &tableName, FunctionExecute exec);

private:
    proto::DatabaseConfig _databaseConfig;
    std::map<std::string, std::unique_ptr<Table>> _tables;
    std::map<std::string, std::unique_ptr<TableGroup>> _tableGroups;
    std::map<std::string, std::unique_ptr<Function>> _functions;
    AUTIL_LOG_DECLARE();
};

extern template Status Database::toProto<>(proto::Database *, proto::DetailLevel::Code) const;
extern template Status Database::toProto<>(proto::DatabaseRecord *, proto::DetailLevel::Code) const;

} // namespace catalog
