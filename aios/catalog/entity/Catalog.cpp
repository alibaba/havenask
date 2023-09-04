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
#include "catalog/entity/Catalog.h"

#include "catalog/util/ProtoUtil.h"

namespace catalog {
AUTIL_LOG_SETUP(catalog, Catalog);

void Catalog::copyDetail(Catalog *other) const {
    other->_catalogName = _catalogName;
    other->_catalogConfig = _catalogConfig;
    for (const auto &it : _databases) {
        other->_databases.emplace(it.first, it.second->clone());
    }
}

Status Catalog::fromProto(const proto::Catalog &proto) {
    _status = proto.status();
    _version = proto.version();
    return fromProtoOrCreate(proto, false);
}

template <typename T>
Status Catalog::toProto(T *proto, proto::DetailLevel::Code detailLevel) const {
    proto->mutable_status()->CopyFrom(_status);
    proto->set_version(_version);
    if (detailLevel == proto::DetailLevel::MINIMAL) {
        return StatusBuilder::ok();
    }
    proto->set_catalog_name(_catalogName);
    proto->mutable_catalog_config()->CopyFrom(_catalogConfig);
    auto nextDetailLevel = detailLevel == proto::DetailLevel::SUMMARY ? proto::DetailLevel::MINIMAL : detailLevel;
    auto names = listDatabase();
    std::sort(names.begin(), names.end());
    for (const auto &name : names) {
        auto it = _databases.find(name);
        CATALOG_CHECK(it != _databases.end(), Status::INTERNAL_ERROR);
        if constexpr (std::is_same_v<std::decay_t<T>, proto::Catalog>) {
            CATALOG_CHECK_OK(it->second->toProto(proto->add_databases(), nextDetailLevel));
        } else if constexpr (std::is_same_v<std::decay_t<T>, proto::CatalogRecord>) {
            auto record = proto->add_database_records();
            record->set_entity_name(name);
            record->set_version(it->second->version());
        } else {
            static_assert(!sizeof(T), "unsupported proto type");
        }
    }
    proto->mutable_operation_meta()->CopyFrom(_operationMeta);
    return StatusBuilder::ok();
}

template Status Catalog::toProto<>(proto::Catalog *, proto::DetailLevel::Code) const;
template Status Catalog::toProto<>(proto::CatalogRecord *, proto::DetailLevel::Code) const;

Status Catalog::compare(const Catalog *other, DiffResult &diffResult) const {
    if (other != nullptr && other->version() == version()) {
        return StatusBuilder::ok();
    }

    for (const auto &it : databases()) {
        const Database *entity = nullptr;
        if (other != nullptr) {
            const auto &entities = other->databases();
            auto otherId = entities.find(it.first);
            if (otherId != entities.end()) {
                entity = otherId->second.get();
            }
        }
        CATALOG_CHECK_OK(it.second->compare(entity, diffResult));
    }
    auto proto = std::make_unique<proto::CatalogRecord>();
    CATALOG_CHECK_OK(toProto(proto.get()));
    diffResult.catalog = std::move(proto);
    return StatusBuilder::ok();
}

void Catalog::updateChildStatus(const proto::EntityStatus &target, UpdateResult &result) {
    for (const auto &[_, database] : databases()) {
        database->updateStatus(target, result);
    }
}

void Catalog::alignVersion(CatalogVersion version) {
    for (auto &it : databases()) {
        it.second->alignVersion(version);
    }
    if (_version == kToUpdateCatalogVersion) {
        _version = version;
    }
}

Status Catalog::create(const proto::Catalog &request) {
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return fromProtoOrCreate(request, true);
}

Status Catalog::update(const proto::Catalog &request) {
    CATALOG_CHECK(!ProtoUtil::compareProto(_catalogConfig, request.catalog_config()),
                  Status::INVALID_ARGUMENTS,
                  "catalog_config has no actual diff for catalog:[",
                  _catalogName,
                  "]");
    _catalogConfig = request.catalog_config();
    _operationMeta = request.operation_meta();
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status Catalog::drop() {
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_DELETE);
    return StatusBuilder::ok();
}

Status Catalog::createDatabase(const proto::Database &request) {
    const auto &databaseName = request.database_name();
    auto result = _databases.try_emplace(databaseName, std::make_unique<Database>());
    CATALOG_CHECK(result.second,
                  Status::DUPLICATE_ENTITY,
                  "database:[",
                  databaseName,
                  "] already exists in catalog:[",
                  _catalogName,
                  "]");
    CATALOG_CHECK_OK(result.first->second->create(request));
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status Catalog::dropDatabase(const DatabaseId &id) {
    return execute(id.databaseName, [&](Database &database) {
        CATALOG_CHECK_OK(database.drop());
        _databases.erase(id.databaseName);
        return StatusBuilder::ok();
    });
}

Status Catalog::getDatabase(const std::string &databaseName, Database *&database) const {
    auto it = _databases.find(databaseName);
    CATALOG_CHECK(it != _databases.end(),
                  Status::NOT_FOUND,
                  "database:[",
                  databaseName,
                  "] not exists in catalog:[",
                  _catalogName,
                  "]");
    database = it->second.get();
    return StatusBuilder::ok();
}

std::vector<std::string> Catalog::listDatabase() const {
    std::vector<std::string> databaseNames;
    for (const auto &it : _databases) {
        databaseNames.emplace_back(it.first);
    }
    return databaseNames;
}

Status Catalog::updateDatabase(const proto::Database &request) {
    return execute(request.database_name(), [&](Database &database) { return database.update(request); });
}

Status Catalog::createTable(const proto::Table &request) {
    return execute(request.database_name(), [&](Database &database) { return database.createTable(request); });
}

Status Catalog::dropTable(const TableId &id) {
    return execute(id.databaseName, [&](Database &database) { return database.dropTable(id); });
}

Status Catalog::updateTable(const proto::Table &request) {
    return execute(request.database_name(), [&](Database &database) { return database.updateTable(request); });
}

Status Catalog::updateTableStructure(const proto::TableStructure &request) {
    return execute(request.database_name(), [&](Database &database) { return database.updateTableStructure(request); });
}

Status Catalog::addColumn(const proto::AddColumnRequest &request) {
    return execute(request.database_name(), [&](Database &database) { return database.addColumn(request); });
}

Status Catalog::updateColumn(const proto::UpdateColumnRequest &request) {
    return execute(request.database_name(), [&](Database &database) { return database.updateColumn(request); });
}

Status Catalog::dropColumn(const proto::DropColumnRequest &request) {
    return execute(request.database_name(), [&](Database &database) { return database.dropColumn(request); });
}

Status Catalog::createIndex(const proto::CreateIndexRequest &request) {
    return execute(request.database_name(), [&](Database &database) { return database.createIndex(request); });
}

Status Catalog::updateIndex(const proto::UpdateIndexRequest &request) {
    return execute(request.database_name(), [&](Database &database) { return database.updateIndex(request); });
}

Status Catalog::dropIndex(const proto::DropIndexRequest &request) {
    return execute(request.database_name(), [&](Database &database) { return database.dropIndex(request); });
}

Status Catalog::createPartition(const proto::CreatePartitionRequest &request) {
    return execute(request.partition().database_name(),
                   [&](Database &database) { return database.createPartition(request); });
}

Status Catalog::updatePartition(const proto::UpdatePartitionRequest &request) {
    return execute(request.partition().database_name(),
                   [&](Database &database) { return database.updatePartition(request); });
}

Status Catalog::updatePartitionTableStructure(const proto::UpdatePartitionTableStructureRequest &request) {
    return execute(request.database_name(),
                   [&](Database &database) { return database.updatePartitionTableStructure(request); });
}

Status Catalog::dropPartition(const proto::DropPartitionRequest &request) {
    return execute(request.database_name(), [&](Database &database) { return database.dropPartition(request); });
}

Status Catalog::createTableGroup(const proto::TableGroup &request) {
    return execute(request.database_name(), [&](Database &database) { return database.createTableGroup(request); });
}

Status Catalog::updateTableGroup(const proto::TableGroup &request) {
    return execute(request.database_name(), [&](Database &database) { return database.updateTableGroup(request); });
}

Status Catalog::dropTableGroup(const TableGroupId &id) {
    return execute(id.databaseName, [&](Database &database) { return database.dropTableGroup(id); });
}

Status Catalog::createLoadStrategy(const proto::LoadStrategy &request) {
    return execute(request.database_name(), [&](Database &database) { return database.createLoadStrategy(request); });
}

Status Catalog::updateLoadStrategy(const proto::LoadStrategy &request) {
    return execute(request.database_name(), [&](Database &database) { return database.updateLoadStrategy(request); });
}

Status Catalog::dropLoadStrategy(const LoadStrategyId &id) {
    return execute(id.databaseName, [&](Database &database) { return database.dropLoadStrategy(id); });
}

Status Catalog::createFunction(const proto::Function &request) {
    return execute(request.database_name(), [&](Database &database) { return database.createFunction(request); });
}

Status Catalog::updateFunction(const proto::Function &request) {
    return execute(request.database_name(), [&](Database &database) { return database.updateFunction(request); });
}

Status Catalog::dropFunction(const FunctionId &id) {
    return execute(id.databaseName, [&](Database &database) { return database.dropFunction(id); });
}

Status Catalog::fromProtoOrCreate(const proto::Catalog &proto, bool create) {
    _id = proto.catalog_name();
    _catalogName = proto.catalog_name();
    CATALOG_CHECK(!_catalogName.empty(), Status::INVALID_ARGUMENTS, "catalog_name is not specified");
    _catalogConfig = proto.catalog_config();
    for (const auto &subProto : proto.databases()) {
        auto element = std::make_unique<Database>();
        if (create) {
            CATALOG_CHECK_OK(element->create(subProto));
        } else {
            CATALOG_CHECK_OK(element->fromProto(subProto));
        }
        _databases.emplace(subProto.database_name(), std::move(element));
    }
    _operationMeta = proto.operation_meta();
    return StatusBuilder::ok();
}

Status Catalog::execute(const std::string &databaseName, DatabaseExecute exec) {
    Database *database = nullptr;
    CATALOG_CHECK_OK(getDatabase(databaseName, database));
    CATALOG_CHECK_OK(exec(*database));
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

} // namespace catalog
