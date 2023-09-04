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
#include "catalog/entity/Database.h"

#include "autil/StringUtil.h"
#include "catalog/util/ProtoUtil.h"

namespace catalog {
AUTIL_LOG_SETUP(catalog, Database);

static const char *kDefaultTableGroupName = "default";

void Database::copyDetail(Database *other) const {
    other->_databaseConfig = _databaseConfig;
    for (const auto &it : _tables) {
        other->_tables.emplace(it.first, it.second->clone());
    }
    for (const auto &it : _tableGroups) {
        other->_tableGroups.emplace(it.first, it.second->clone());
    }
    for (const auto &it : _functions) {
        other->_functions.emplace(it.first, it.second->clone());
    }
}

template <typename T>
inline Status checkSubProto(const DatabaseId &id, const T &subProto) {
    CATALOG_CHECK(id.databaseName == subProto.database_name(),
                  Status::INVALID_ARGUMENTS,
                  "database_name:[",
                  subProto.database_name(),
                  "] conflict with database:[",
                  id.databaseName,
                  "] in catalog:[",
                  id.catalogName,
                  "]");
    CATALOG_CHECK(id.catalogName == subProto.catalog_name(),
                  Status::INVALID_ARGUMENTS,
                  "catalog_name:[",
                  subProto.catalog_name(),
                  "] conflict with database:[",
                  id.databaseName,
                  "] in catalog:[",
                  id.catalogName,
                  "]");
    return StatusBuilder::ok();
}

Status Database::fromProto(const proto::Database &proto) {
    _status = proto.status();
    _version = proto.version();
    return fromProtoOrCreate(proto, false);
}

template <typename T>
Status Database::toProto(T *proto, proto::DetailLevel::Code detailLevel) const {
    proto->mutable_status()->CopyFrom(_status);
    proto->set_version(_version);
    proto->set_database_name(_id.databaseName);
    if (detailLevel == proto::DetailLevel::MINIMAL) {
        return StatusBuilder::ok();
    }
    proto->set_catalog_name(_id.catalogName);
    proto->mutable_database_config()->CopyFrom(_databaseConfig);
    auto nextDetailLevel = detailLevel == proto::DetailLevel::SUMMARY ? proto::DetailLevel::MINIMAL : detailLevel;
    {
        auto names = listTable();
        std::sort(names.begin(), names.end());
        for (const auto &name : names) {
            auto it = _tables.find(name);
            CATALOG_CHECK(it != _tables.end(), Status::INTERNAL_ERROR);
            if constexpr (std::is_same_v<std::decay_t<T>, proto::Database>) {
                CATALOG_CHECK_OK(it->second->toProto(proto->add_tables(), nextDetailLevel));
            } else if constexpr (std::is_same_v<std::decay_t<T>, proto::DatabaseRecord>) {
                auto record = proto->add_table_records();
                record->set_entity_name(name);
                record->set_version(it->second->version());
            } else {
                static_assert(!sizeof(T), "unsupported proto type");
            }
        }
    }
    {
        auto names = listTableGroup();
        std::sort(names.begin(), names.end());
        for (const auto &name : names) {
            auto it = _tableGroups.find(name);
            CATALOG_CHECK(it != _tableGroups.end(), Status::INTERNAL_ERROR);
            if constexpr (std::is_same_v<std::decay_t<T>, proto::Database>) {
                CATALOG_CHECK_OK(it->second->toProto(proto->add_table_groups(), nextDetailLevel));
            } else if constexpr (std::is_same_v<std::decay_t<T>, proto::DatabaseRecord>) {
                auto record = proto->add_table_group_records();
                record->set_entity_name(name);
                record->set_version(it->second->version());
            } else {
                static_assert(!sizeof(T), "unsupported proto type");
            }
        }
    }
    {
        auto names = listFunction();
        std::sort(names.begin(), names.end());
        for (const auto &name : names) {
            auto it = _functions.find(name);
            CATALOG_CHECK(it != _functions.end(), Status::INTERNAL_ERROR);
            if constexpr (std::is_same_v<std::decay_t<T>, proto::Database>) {
                CATALOG_CHECK_OK(it->second->toProto(proto->add_functions(), nextDetailLevel));
            } else if constexpr (std::is_same_v<std::decay_t<T>, proto::DatabaseRecord>) {
                auto record = proto->add_function_records();
                record->set_entity_name(name);
                record->set_version(it->second->version());
            } else {
                static_assert(!sizeof(T), "unsupported proto type");
            }
        }
    }
    proto->mutable_operation_meta()->CopyFrom(_operationMeta);
    return StatusBuilder::ok();
}

template Status Database::toProto<>(proto::Database *, proto::DetailLevel::Code) const;
template Status Database::toProto<>(proto::DatabaseRecord *, proto::DetailLevel::Code) const;

Status Database::compare(const Database *other, DiffResult &diffResult) const {
    if (other != nullptr && other->version() == version()) {
        return StatusBuilder::ok();
    }
    for (const auto &it : tables()) {
        const Table *entity = nullptr;
        if (other != nullptr) {
            const auto &entities = other->tables();
            auto otherId = entities.find(it.first);
            if (otherId != entities.end()) {
                entity = otherId->second.get();
            }
        }
        CATALOG_CHECK_OK(it.second->compare(entity, diffResult));
    }
    for (const auto &it : tableGroups()) {
        const TableGroup *entity = nullptr;
        if (other != nullptr) {
            const auto &entities = other->tableGroups();
            auto otherId = entities.find(it.first);
            if (otherId != entities.end()) {
                entity = otherId->second.get();
            }
        }
        CATALOG_CHECK_OK(it.second->compare(entity, diffResult));
    }
    for (const auto &it : functions()) {
        const Function *entity = nullptr;
        if (other != nullptr) {
            const auto &entities = other->functions();
            auto otherId = entities.find(it.first);
            if (otherId != entities.end()) {
                entity = otherId->second.get();
            }
        }
        CATALOG_CHECK_OK(it.second->compare(entity, diffResult));
    }
    auto proto = std::make_unique<proto::DatabaseRecord>();
    CATALOG_CHECK_OK(toProto(proto.get()));
    diffResult.databases.emplace_back(std::move(proto));
    return StatusBuilder::ok();
}

void Database::updateChildStatus(const proto::EntityStatus &target, UpdateResult &result) {
    for (const auto &[_, table] : tables()) {
        table->updateStatus(target, result);
    }

    for (const auto &[_, tableGroup] : tableGroups()) {
        tableGroup->updateStatus(target, result);
    }

    for (const auto &[_, function] : functions()) {
        function->updateStatus(target, result);
    }
}

void Database::alignVersion(CatalogVersion version) {
    for (auto &it : tables()) {
        it.second->alignVersion(version);
    }
    for (auto &it : tableGroups()) {
        it.second->alignVersion(version);
    }
    for (auto &it : functions()) {
        it.second->alignVersion(version);
    }
    if (_version == kToUpdateCatalogVersion) {
        _version = version;
    }
}

Status Database::create(const proto::Database &request) {
    CATALOG_CHECK_OK(fromProtoOrCreate(request, true));
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status Database::update(const proto::Database &request) {
    CATALOG_CHECK(!ProtoUtil::compareProto(_databaseConfig, request.database_config()),
                  Status::INVALID_ARGUMENTS,
                  "database_config has no actual diff for database:[",
                  _id.databaseName,
                  "]");
    _databaseConfig = request.database_config();
    _operationMeta = request.operation_meta();
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status Database::drop() {
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_DELETE);
    return StatusBuilder::ok();
}

Status Database::createTable(const proto::Table &request) {
    CATALOG_CHECK_OK(addEntityCheck(request));
    auto table = std::make_unique<Table>();
    CATALOG_CHECK_OK(table->create(request));
    _tables.emplace(request.table_name(), std::move(table));
    // TODO(chekong.ygm): 是否允许default table_group被删除的情况
    if (_tableGroups.find(kDefaultTableGroupName) != _tableGroups.end()) {
        // TODO(chekong.ygm): custom default LoadStrategy
        proto::LoadStrategy loadStrategy;
        loadStrategy.set_table_name(request.table_name());
        loadStrategy.set_table_group_name(kDefaultTableGroupName);
        loadStrategy.set_database_name(_id.databaseName);
        loadStrategy.set_catalog_name(_id.catalogName);
        CATALOG_CHECK_OK(createLoadStrategy(loadStrategy));
    }
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status Database::dropTable(const TableId &id) {
    return execute(id.tableName, [&](Table &table) {
        for (const auto &it : _tableGroups) {
            if (it.second->hasLoadStrategy(id.tableName)) {
                CATALOG_CHECK_OK(
                    it.second->dropLoadStrategy({id.tableName, it.first, _id.databaseName, _id.catalogName}));
            }
        }
        CATALOG_CHECK_OK(table.drop());
        _tables.erase(id.tableName);
        return StatusBuilder::ok();
    });
}

Status Database::updateTable(const proto::Table &request) {
    return execute(request.table_name(), [&](Table &table) { return table.update(request); });
}

Status Database::getTable(const std::string &tableName, Table *&table) const {
    auto it = _tables.find(tableName);
    CATALOG_CHECK(it != _tables.end(),
                  Status::NOT_FOUND,
                  "table:[",
                  tableName,
                  "] not exists in database:[",
                  _id.databaseName,
                  "]");
    table = it->second.get();
    return StatusBuilder::ok();
}

std::vector<std::string> Database::listTable() const {
    std::vector<std::string> tableNames;
    for (const auto &it : _tables) {
        tableNames.emplace_back(it.first);
    }
    return tableNames;
}

Status Database::updateTableStructure(const proto::TableStructure &request) {
    return execute(request.table_name(), [&](Table &table) { return table.updateTableStructure(request); });
}

Status Database::addColumn(const proto::AddColumnRequest &request) {
    return execute(request.table_name(), [&](Table &table) { return table.addColumn(request); });
}

Status Database::updateColumn(const proto::UpdateColumnRequest &request) {
    return execute(request.table_name(), [&](Table &table) { return table.updateColumn(request); });
}

Status Database::dropColumn(const proto::DropColumnRequest &request) {
    return execute(request.table_name(), [&](Table &table) { return table.dropColumn(request); });
}

Status Database::createIndex(const proto::CreateIndexRequest &request) {
    return execute(request.table_name(), [&](Table &table) { return table.createIndex(request); });
}

Status Database::updateIndex(const proto::UpdateIndexRequest &request) {
    return execute(request.table_name(), [&](Table &table) { return table.updateIndex(request); });
}

Status Database::dropIndex(const proto::DropIndexRequest &request) {
    return execute(request.table_name(), [&](Table &table) { return table.dropIndex(request); });
}

Status Database::createPartition(const proto::CreatePartitionRequest &request) {
    auto partitionType = request.partition().partition_type();
    if (partitionType == proto::PartitionType::TABLE_PARTITION) {
        Table *table = nullptr;
        CATALOG_CHECK_OK(getTable(request.partition().table_name(), table));
        CATALOG_CHECK_OK(table->createPartition(request.partition()));
    } else {
        CATALOG_CHECK(false,
                      Status::INVALID_ARGUMENTS,
                      "unsupported partition_type:[",
                      proto::PartitionType_Code_Name(partitionType),
                      "]");
    }
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status Database::updatePartition(const proto::UpdatePartitionRequest &request) {
    auto partitionType = request.partition().partition_type();
    if (partitionType == proto::PartitionType::TABLE_PARTITION) {
        Table *table = nullptr;
        CATALOG_CHECK_OK(getTable(request.partition().table_name(), table));
        CATALOG_CHECK_OK(table->updatePartition(request.partition()));
    } else {
        CATALOG_CHECK(false,
                      Status::INVALID_ARGUMENTS,
                      "unsupported partition_type:[",
                      proto::PartitionType_Code_Name(partitionType),
                      "]");
    }
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status Database::updatePartitionTableStructure(const proto::UpdatePartitionTableStructureRequest &request) {
    return execute(request.table_name(), [&](Table &table) { return table.updatePartitionTableStructure(request); });
}

Status Database::dropPartition(const proto::DropPartitionRequest &request) {
    PartitionId pId{request.partition_name(),
                    request.table_name(),
                    request.database_name(),
                    request.catalog_name(),
                    request.partition_type()};
    auto partitionType = request.partition_type();
    if (partitionType == proto::PartitionType::TABLE_PARTITION) {
        Table *table = nullptr;
        CATALOG_CHECK_OK(getTable(request.table_name(), table));
        CATALOG_CHECK_OK(table->dropPartition(pId));
    } else {
        CATALOG_CHECK(false,
                      Status::INVALID_ARGUMENTS,
                      "unsupported partition_type:[",
                      proto::PartitionType_Code_Name(partitionType),
                      "]");
    }
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status Database::createTableGroup(const proto::TableGroup &request) {
    CATALOG_CHECK_OK(addEntityCheck(request));
    auto tableGroup = std::make_unique<TableGroup>();
    CATALOG_CHECK_OK(tableGroup->create(request));
    _tableGroups.emplace(request.table_group_name(), std::move(tableGroup));
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status Database::updateTableGroup(const proto::TableGroup &request) {
    return execute(request.table_group_name(), [&](TableGroup &tableGroup) { return tableGroup.update(request); });
}

Status Database::dropTableGroup(const TableGroupId &id) {
    return execute(id.tableGroupName, [&](TableGroup &tableGroup) {
        CATALOG_CHECK_OK(tableGroup.drop());
        _tableGroups.erase(id.tableGroupName);
        return StatusBuilder::ok();
    });
}

Status Database::getTableGroup(const std::string &tableGroupName, TableGroup *&tableGroup) const {
    auto it = _tableGroups.find(tableGroupName);
    CATALOG_CHECK(it != _tableGroups.end(),
                  Status::NOT_FOUND,
                  "table_group:[",
                  tableGroupName,
                  "] not exists in database:[",
                  _id.databaseName,
                  "]");
    tableGroup = it->second.get();
    return StatusBuilder::ok();
}

std::vector<std::string> Database::listTableGroup() const {
    std::vector<std::string> tableGroupNames;
    for (const auto &it : _tableGroups) {
        tableGroupNames.emplace_back(it.first);
    }
    return tableGroupNames;
}

Status Database::createLoadStrategy(const proto::LoadStrategy &request) {
    CATALOG_CHECK_OK(addEntityCheck(request));
    return execute(request.table_group_name(),
                   [&](TableGroup &tableGroup) { return tableGroup.createLoadStrategy(request); });
}

Status Database::updateLoadStrategy(const proto::LoadStrategy &request) {
    return execute(request.table_group_name(),
                   [&](TableGroup &tableGroup) { return tableGroup.updateLoadStrategy(request); });
}

Status Database::dropLoadStrategy(const LoadStrategyId &id) {
    return execute(id.tableGroupName, [&](TableGroup &tableGroup) { return tableGroup.dropLoadStrategy(id); });
}

Status Database::createFunction(const proto::Function &request) {
    CATALOG_CHECK_OK(addEntityCheck(request));
    auto function = std::make_unique<Function>();
    CATALOG_CHECK_OK(function->create(request));
    _functions.emplace(request.function_name(), std::move(function));
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status Database::updateFunction(const proto::Function &request) {
    return execute(request.function_name(), [&](Function &function) { return function.update(request); });
}

Status Database::dropFunction(const FunctionId &id) {
    return execute(id.functionName, [&](Function &function) {
        CATALOG_CHECK_OK(function.drop());
        _functions.erase(id.functionName);
        return StatusBuilder::ok();
    });
}

Status Database::getFunction(const std::string &functionName, Function *&function) const {
    auto it = _functions.find(functionName);
    CATALOG_CHECK(it != _functions.end(),
                  Status::NOT_FOUND,
                  "function:[",
                  functionName,
                  "] not exists in database:[",
                  _id.databaseName,
                  "]");
    function = it->second.get();
    return StatusBuilder::ok();
}

std::vector<std::string> Database::listFunction() const {
    std::vector<std::string> functionNames;
    for (const auto &it : _functions) {
        functionNames.emplace_back(it.first);
    }
    return functionNames;
}

Status Database::fromProtoOrCreate(const proto::Database &proto, bool create) {
    _id.databaseName = proto.database_name();
    CATALOG_CHECK(!_id.databaseName.empty(), Status::INVALID_ARGUMENTS, "database_name is not specified");
    _id.catalogName = proto.catalog_name();
    CATALOG_CHECK(!_id.catalogName.empty(), Status::INVALID_ARGUMENTS, "catalog_name is not specified");
    _databaseConfig = proto.database_config();
    // 注意不能随意调整几个for loop的顺序，table_group.load_strategy依赖table
    for (const auto &subProto : proto.tables()) {
        CATALOG_CHECK_OK(addEntityCheck(subProto));
        auto element = std::make_unique<Table>();
        if (create) {
            CATALOG_CHECK_OK(element->create(subProto));
        } else {
            CATALOG_CHECK_OK(element->fromProto(subProto));
        }
        _tables.emplace(subProto.table_name(), std::move(element));
    }
    for (const auto &subProto : proto.table_groups()) {
        CATALOG_CHECK_OK(addEntityCheck(subProto));
        auto element = std::make_unique<TableGroup>();
        if (create) {
            CATALOG_CHECK_OK(element->create(subProto));
        } else {
            CATALOG_CHECK_OK(element->fromProto(subProto));
        }
        _tableGroups.emplace(subProto.table_group_name(), std::move(element));
    }
    for (const auto &subProto : proto.functions()) {
        CATALOG_CHECK_OK(addEntityCheck(subProto));
        auto element = std::make_unique<Function>();
        if (create) {
            CATALOG_CHECK_OK(element->create(subProto));
        } else {
            CATALOG_CHECK_OK(element->fromProto(subProto));
        }
        _functions.emplace(subProto.function_name(), std::move(element));
    }
    _operationMeta = proto.operation_meta();
    return StatusBuilder::ok();
}

Status Database::addEntityCheck(const proto::Table &proto) {
    CATALOG_CHECK_OK(checkSubProto(_id, proto));
    const auto &tableName = proto.table_name();
    CATALOG_CHECK(_tables.find(tableName) == _tables.end(),
                  Status::DUPLICATE_ENTITY,
                  "table:[",
                  tableName,
                  "] already exists in database:[",
                  _id.databaseName,
                  "] with catalog:[",
                  _id.catalogName,
                  "]");
    return StatusBuilder::ok();
}

Status Database::addEntityCheck(const proto::LoadStrategy &proto) {
    CATALOG_CHECK_OK(checkSubProto(_id, proto));
    CATALOG_CHECK(_tables.find(proto.table_name()) != _tables.end(),
                  Status::NOT_FOUND,
                  "table:[",
                  proto.table_name(),
                  "] required by load_strategy not exists with table_group:[",
                  _id.databaseName,
                  ".",
                  proto.table_group_name(),
                  "] and catalog:[",
                  _id.catalogName,
                  "]");
    return StatusBuilder::ok();
}

Status Database::addEntityCheck(const proto::TableGroup &proto) {
    CATALOG_CHECK_OK(checkSubProto(_id, proto));
    const auto &name = proto.table_group_name();
    CATALOG_CHECK(_tableGroups.find(name) == _tableGroups.end(),
                  Status::DUPLICATE_ENTITY,
                  "table_group:[",
                  name,
                  "] already exists in database:[",
                  _id.databaseName,
                  "] with catalog:[",
                  _id.catalogName,
                  "]");
    for (const auto &subProto : proto.load_strategies()) {
        CATALOG_CHECK_OK(addEntityCheck(subProto));
    }
    return StatusBuilder::ok();
}

Status Database::addEntityCheck(const proto::Function &proto) {
    CATALOG_CHECK_OK(checkSubProto(_id, proto));
    const auto &name = proto.function_name();
    CATALOG_CHECK(_functions.find(name) == _functions.end(),
                  Status::DUPLICATE_ENTITY,
                  "function:[",
                  name,
                  "] already exists in database:[",
                  _id.databaseName,
                  "] with catalog:[",
                  _id.catalogName,
                  "]");
    return StatusBuilder::ok();
}

Status Database::execute(const std::string &tableName, TableExecute exec) {
    Table *table = nullptr;
    CATALOG_CHECK_OK(getTable(tableName, table));
    CATALOG_CHECK_OK(exec(*table));
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status Database::execute(const std::string &tableGroupName, TableGroupExecute exec) {
    TableGroup *tableGroup = nullptr;
    CATALOG_CHECK_OK(getTableGroup(tableGroupName, tableGroup));
    CATALOG_CHECK_OK(exec(*tableGroup));
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status Database::execute(const std::string &functionName, FunctionExecute exec) {
    Function *function = nullptr;
    CATALOG_CHECK_OK(getFunction(functionName, function));
    CATALOG_CHECK_OK(exec(*function));
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status Database::getTableGroupTables(const std::string &tableGroupName, std::vector<const Table *> &tables) {
    if (_tableGroups.count(tableGroupName) == 0) {
        return StatusBuilder::make(Status::INTERNAL_ERROR, "table group name " + tableGroupName + " not found");
    }
    if (_tableGroups.size() == 1) {
        for (const auto &iter : _tables) {
            tables.push_back(iter.second.get());
        }
        return StatusBuilder::ok();
    }

    auto tg = _tableGroups[tableGroupName].get();
    for (const auto &iter : tg->loadStrategies()) {
        if (_tables.count(iter.first) != 0) {
            tables.push_back(_tables[iter.first].get());
        }
    }

    return StatusBuilder::ok();
}

} // namespace catalog
