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
#include <vector>

#include "autil/Log.h"
#include "catalog/entity/Database.h"
#include "catalog/entity/DiffResult.h"
#include "catalog/entity/EntityBase.h"
#include "catalog/entity/Type.h"
#include "catalog/entity/UpdateResult.h"
#include "catalog/proto/CatalogService.pb.h"
#include "catalog/util/StatusBuilder.h"

namespace catalog {

SPECIALIZE_TRAITS(Catalog, CatalogId);

class Catalog : public EntityBase<Catalog> {
public:
    Catalog() = default;
    ~Catalog() = default;
    Catalog(const Catalog &) = delete;
    Catalog &operator=(const Catalog &) = delete;

public:
    const auto &catalogName() const { return _catalogName; }
    const auto &catalogConfig() const { return _catalogConfig; }
    const auto &databases() const { return _databases; };

    void copyDetail(Catalog *other) const;
    Status fromProto(const proto::Catalog &proto);
    template <typename T>
    Status toProto(T *proto, proto::DetailLevel::Code detailLevel = proto::DetailLevel::DETAILED) const;
    Status compare(const Catalog *other, DiffResult &diffResult) const;
    void updateChildStatus(const proto::EntityStatus &target, UpdateResult &result);
    void alignVersion(CatalogVersion version);

    // ---------------- API for Catalog --------------------
    Status create(const proto::Catalog &request);
    Status update(const proto::Catalog &request);
    Status drop();

    // ---------------- API for Database -------------------
    Status createDatabase(const proto::Database &request);
    Status updateDatabase(const proto::Database &request);
    Status dropDatabase(const DatabaseId &id);
    Status getDatabase(const std::string &databaseName, Database *&database) const;
    bool hasDatabase(const std::string &databaseName) const {
        return _databases.find(databaseName) != _databases.end();
    }
    std::vector<std::string> listDatabase() const;

    // ---------------- API for Table ----------------------
    Status createTable(const proto::Table &request);
    Status dropTable(const TableId &id);
    Status updateTable(const proto::Table &request);

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
    Status createLoadStrategy(const proto::LoadStrategy &request);
    Status updateLoadStrategy(const proto::LoadStrategy &request);
    Status dropLoadStrategy(const LoadStrategyId &id);

    // ---------------- API for Function -------------------
    Status createFunction(const proto::Function &request);
    Status updateFunction(const proto::Function &request);
    Status dropFunction(const FunctionId &id);

private:
    Status fromProtoOrCreate(const proto::Catalog &proto, bool create);
    using DatabaseExecute = std::function<Status(Database &)>;
    Status execute(const std::string &databaseName, DatabaseExecute exec);

private:
    std::string _catalogName;
    proto::CatalogConfig _catalogConfig;
    std::map<std::string, std::unique_ptr<Database>> _databases;
    AUTIL_LOG_DECLARE();
};

extern template Status Catalog::toProto<>(proto::Catalog *, proto::DetailLevel::Code) const;
extern template Status Catalog::toProto<>(proto::CatalogRecord *, proto::DetailLevel::Code) const;

} // namespace catalog
