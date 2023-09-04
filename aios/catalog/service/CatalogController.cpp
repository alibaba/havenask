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
#include "catalog/service/CatalogController.h"

#include "autil/EnvUtil.h"
#include "catalog/tools/BSConfigMaker.h"
#include "catalog/util/ProtoUtil.h"
#include "fslib/fs/FileSystem.h"

namespace catalog {
AUTIL_LOG_SETUP(catalog, CatalogController);

CatalogController::CatalogController(const std::string &catalogName, const std::shared_ptr<IStore> &store)
    : _catalogName(catalogName), _store(store) {}

CatalogController::~CatalogController() {}

bool CatalogController::init() {
    _reader = _store->createReader(_catalogName);
    if (_reader == nullptr) {
        return false;
    }

    _writer = _store->createWriter(_catalogName);
    if (_writer == nullptr) {
        return false;
    }
    _syncThread = autil::LoopThread::createLoopThread(std::bind(&CatalogController::syncLoop, this), 2 * 1000 * 1000);
    if (!_syncThread) {
        AUTIL_LOG(ERROR, "create sync loop thread failed");
        return false;
    }
    return true;
}

bool CatalogController::recover() {
    CatalogVersion version = kInvalidCatalogVersion;
    const auto &status = _reader->getLatestVersion(version);
    if (!isOk(status)) {
        AUTIL_LOG(ERROR, "missing catalog:[%s] with error:[%s]", _catalogName.c_str(), status.message().c_str());
        return false;
    }
    auto snapshot = retrieveSnapshot(version);
    if (snapshot == nullptr) {
        return false;
    }
    {
        autil::ScopedWriteLock lock(_buildLock);
        auto status = _reader->read(&_builds);
        if (!isOk(status)) {
            AUTIL_LOG(ERROR,
                      "build of catalog [%s] read failed, with error:[%s]",
                      _catalogName.c_str(),
                      status.message().c_str());
            return false;
        }
    }
    // set _snapshot in the end,
    // to make sure recover is finished when sync thread see a valid snapshot
    {
        autil::ScopedWriteLock lock(_lock);
        _snapshot = std::move(snapshot);
    }

    return true;
}

Status CatalogController::getCurrentPartitions(const Catalog *catalog,
                                               std::map<PartitionId, const Partition *> &partitions) {
    for (const auto &[_, database] : catalog->databases()) {
        for (const auto &[_, table] : database->tables()) {
            for (const auto &[_, partition] : table->partitions()) {
                const auto &id = partition->id();
                PartitionId partitionId = {id.partitionName, id.tableName, id.databaseName, id.catalogName};
                partitions[partitionId] = partition.get();
            }
        }
    }
    return StatusBuilder::ok();
}

Status CatalogController::getCurrentBuildIds(const std::string &catalogName,
                                             std::map<PartitionId, uint32_t> &buildIds) {
    proto::ListBuildRequest request;
    request.set_catalog_name(catalogName);
    proto::ListBuildResponse response;
    listBuild(&request, &response);
    CATALOG_CHECK_OK(response.status());
    buildIds.clear();
    for (int i = 0; i < response.build_ids_size(); ++i) {
        const auto &buildId = response.build_ids(i);
        PartitionId partitionId = {
            buildId.partition_name(), buildId.table_name(), buildId.database_name(), buildId.catalog_name()};
        buildIds[partitionId] = buildId.generation_id();
    }
    return StatusBuilder::ok();
}

void CatalogController::createBuild(const Partition *partition, const std::string &storeRoot) {
    const auto &tableStructure = partition->tableStructure();
    if (!tableStructure || tableStructure->tableStructureConfig().build_type() != proto::BuildType::OFFLINE) {
        return;
    }
    if (storeRoot.empty()) {
        AUTIL_LOG(ERROR, "store root is empty, create build failed");
        return;
    }
    proto::CreateBuildRequest request;
    if (auto status = partition->toProto(request.mutable_partition()); !isOk(status)) {
        AUTIL_LOG(ERROR, "create build failed, [%s]", status.message().c_str());
        return;
    }
    request.set_store_root(storeRoot);
    proto::CreateBuildResponse response;
    createBuild(&request, &response);
    if (response.status().code() != proto::ResponseStatus::OK) {
        AUTIL_LOG(ERROR, "create build failed, [%s]", response.status().message().c_str());
        return;
    }
}

void CatalogController::updateBuild(const Partition *partition, const std::string &storeRoot, uint32_t generationId) {
    proto::UpdateBuildRequest request;
    auto build = request.mutable_build();

    auto buildId = build->mutable_build_id();
    const auto &partitionId = partition->id();
    buildId->set_generation_id(generationId);
    buildId->set_partition_name(partitionId.partitionName);
    buildId->set_table_name(partitionId.tableName);
    buildId->set_database_name(partitionId.databaseName);
    buildId->set_catalog_name(partitionId.catalogName);

    auto target = build->mutable_target();
    auto buildType = partition->tableStructure()->tableStructureConfig().build_type();
    if (buildType == proto::BuildType::OFFLINE) {
        target->set_type(proto::BuildTarget::BATCH_BUILD);
    }
    target->set_build_state(proto::BuildState::RUNNING);
    std::string templatePath;
    if (auto status = getBSTemplateConfigPath(&templatePath); !isOk(status)) {
        AUTIL_LOG(ERROR, "get template config path failed, [%s]", status.message().c_str());
        return;
    }
    std::string configPath;
    auto status = BSConfigMaker::Make(*partition, templatePath, storeRoot, &configPath);
    if (!isOk(status)) {
        AUTIL_LOG(ERROR, "make config failed, [%s]", status.message().c_str());
        return;
    }
    target->set_config_path(configPath);

    proto::BuildCommonResponse response;
    updateBuild(&request, &response);
    if (response.status().code() != proto::ResponseStatus::OK) {
        AUTIL_LOG(ERROR, "update build failed, [%s]", response.status().message().c_str());
        return;
    }
}

void CatalogController::dropBuild(const PartitionId &partitionId) {
    proto::DropBuildRequest request;
    auto buildId = request.mutable_build_id();
    buildId->set_partition_name(partitionId.partitionName);
    buildId->set_table_name(partitionId.tableName);
    buildId->set_database_name(partitionId.databaseName);
    buildId->set_catalog_name(partitionId.catalogName);
    proto::BuildCommonResponse response;
    dropBuild(&request, &response);
    if (response.status().code() != proto::ResponseStatus::OK) {
        AUTIL_LOG(ERROR, "drop build failed, [%s]", response.status().message().c_str());
        return;
    }
}

std::string CatalogController::getStoreRoot(const std::unique_ptr<Catalog> &catalog, const PartitionId &id) {
    for (const auto &[name, database] : catalog->databases()) {
        if (name == id.databaseName) {
            return database->databaseConfig().store_root();
        }
    }
    return "";
}

void CatalogController::syncLoop() {
    auto curCatalog = std::make_unique<Catalog>();
    std::map<PartitionId, const Partition *> partitions;
    if (auto status = getCatalog(curCatalog.get()); !isOk(status)) {
        return;
    }
    if (!isOk(getCurrentPartitions(curCatalog.get(), partitions))) {
        AUTIL_LOG(ERROR, "get current partitions failed");
        return;
    }

    std::map<PartitionId, /*generation id*/ uint32_t> buildIds;
    if (!isOk(getCurrentBuildIds(curCatalog->id(), buildIds))) {
        AUTIL_LOG(ERROR, "get current builds failed");
        return;
    }

    for (const auto &[partitionId, partition] : partitions) {
        auto iter = _lastPartitions.find(partitionId);
        // not change
        if (iter != _lastPartitions.end() && partition->version() == iter->second->version()) {
            continue;
        }
        auto idIter = buildIds.find(partitionId);
        // add partition
        if (idIter == buildIds.end()) {
            createBuild(partition, getStoreRoot(curCatalog, partitionId));
            continue;
        }
        if (partition->getTableStructure()->tableStructureConfig().build_type() != proto::BuildType::OFFLINE) {
            dropBuild(partitionId);
            continue;
        }
        // update partition
        updateBuild(partition, getStoreRoot(curCatalog, partitionId), idIter->second);
    }

    // drop partition
    for (const auto &[partitionId, _] : _lastPartitions) {
        if (partitions.count(partitionId) == 0) {
            dropBuild(partitionId);
        }
    }

    _lastPartitions.swap(partitions);
    _lastCatalog = std::move(curCatalog);
}

const proto::EntityStatus &CatalogController::status() {
    autil::ScopedReadLock lock(_lock);
    return _snapshot->status();
}

#define GET_SNAPSHOT(RESPONSE, CATALOG_VERSION)                                                                        \
    autil::ScopedReadLock lock(_lock);                                                                                 \
    auto snapshot = getSnapshot(_snapshot, (CATALOG_VERSION));                                                         \
    CATALOG_REQUIRES(RESPONSE,                                                                                         \
                     snapshot != nullptr,                                                                              \
                     Status::NOT_FOUND,                                                                                \
                     "missing catalog:[",                                                                              \
                     _catalogName,                                                                                     \
                     "] with version:[",                                                                               \
                     (CATALOG_VERSION),                                                                                \
                     "]")

void CatalogController::getCatalog(const proto::GetCatalogRequest *request, proto::GetCatalogResponse *response) {
    GET_SNAPSHOT(response, request->catalog_version());
    CATALOG_REQUIRES_OK(response, snapshot->toProto(response->mutable_catalog(), request->detail_level()));
}

void CatalogController::createCatalog(const proto::CreateCatalogRequest *request, proto::CommonResponse *response) {
    auto status = checkAndUpdate(
        [](CatalogSnapshot *snapshot) {
            CATALOG_CHECK(snapshot == nullptr, Status::INTERNAL_ERROR, "create catalog check failed");
            return StatusBuilder::ok();
        },
        [&](Catalog &catalog) {
            CATALOG_CHECK_OK(catalog.create(request->catalog()));
            return StatusBuilder::ok();
        },
        [&](CatalogSnapshot *oldSnapshot, CatalogSnapshot *newSnapshot) {
            AUTIL_LOG(INFO, "catalog:[%s] installing new version:[%ld]", _catalogName.c_str(), newSnapshot->version());
            CATALOG_CHECK_OK(_writer->write(oldSnapshot, newSnapshot));
            AUTIL_LOG(INFO, "catalog:[%s] new version:[%ld] installed", _catalogName.c_str(), newSnapshot->version());
            return StatusBuilder::ok();
        },
        [&](CatalogSnapshot *oldSnapshot, CatalogSnapshot *newSnapshot) {
            response->set_source_catalog_version(kInvalidCatalogVersion);
            response->set_target_catalog_version(newSnapshot->version());
            return StatusBuilder::ok();
        });
    CATALOG_REQUIRES_OK(response, status);
}

void CatalogController::dropCatalog(const proto::DropCatalogRequest *request, proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) { return catalog.drop(); });
}

void CatalogController::updateCatalog(const proto::UpdateCatalogRequest *request, proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) { return catalog.update(request->catalog()); });
}
void CatalogController::updateCatalogStatus(const proto::UpdateCatalogStatusRequest *request,
                                            proto::CommonResponse *response) {
    UpdateResult updateResult;
    auto status = checkAndUpdate(
        [&](CatalogSnapshot *snapshot) {
            CATALOG_CHECK(snapshot != nullptr, Status::INTERNAL_ERROR, "old snapshot not exists");
            auto inputVersion = request->catalog_version();
            auto currentVersion = snapshot->version();
            CATALOG_CHECK(currentVersion == inputVersion,
                          Status::VERSION_EXPIRED,
                          "version:[",
                          inputVersion,
                          "] for catalog:[",
                          snapshot->catalogName(),
                          "] expired, expected version:[",
                          currentVersion,
                          "]");
            CATALOG_CHECK(request->status().code() == proto::EntityStatus::PUBLISHED,
                          Status::INVALID_ARGUMENTS,
                          "unsupported status: [",
                          proto::EntityStatus::Code_Name(request->status().code()),
                          "] to update");
            return StatusBuilder::ok();
        },
        [&](Catalog &catalog) { return StatusBuilder::ok(); },
        [&](CatalogSnapshot *oldSnapshot, CatalogSnapshot *newSnapshot) {
            AUTIL_LOG(
                INFO, "catalog:[%s] updating status with version:[%ld]", _catalogName.c_str(), newSnapshot->version());
            CATALOG_CHECK_OK(_writer->updateStatus(newSnapshot, request->status()));
            AUTIL_LOG(
                INFO, "catalog:[%s] status updated with version:[%ld]", _catalogName.c_str(), newSnapshot->version());
            return StatusBuilder::ok();
        },
        [&](CatalogSnapshot *oldSnapshot, CatalogSnapshot *newSnapshot) {
            response->set_source_catalog_version(oldSnapshot->version());
            response->set_target_catalog_version(newSnapshot->version());
            return StatusBuilder::ok();
        });
    CATALOG_REQUIRES_OK(response, status);
}

void CatalogController::listDatabase(const proto::ListDatabaseRequest *request, proto::ListDatabaseResponse *response) {
    GET_SNAPSHOT(response, request->catalog_version());
    const auto &names = snapshot->listDatabase();
    *(response->mutable_database_names()) = {names.begin(), names.end()};
}

void CatalogController::getDatabase(const proto::GetDatabaseRequest *request, proto::GetDatabaseResponse *response) {
    GET_SNAPSHOT(response, request->catalog_version());
    if (request->ignore_not_found_error() && !snapshot->hasDatabase(request->database_name())) {
        return;
    }
    Database *database = nullptr;
    CATALOG_REQUIRES_OK(response, snapshot->getDatabase(request->database_name(), database));
    CATALOG_REQUIRES_OK(response, database->toProto(response->mutable_database(), request->detail_level()));
}

void CatalogController::createDatabase(const proto::CreateDatabaseRequest *request, proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) { return catalog.createDatabase(request->database()); });
}

void CatalogController::dropDatabase(const proto::DropDatabaseRequest *request, proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) {
        return catalog.dropDatabase({request->database_name(), request->catalog_name()});
    });
}

void CatalogController::updateDatabase(const proto::UpdateDatabaseRequest *request, proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) { return catalog.updateDatabase(request->database()); });
}

void CatalogController::listTable(const proto::ListTableRequest *request, proto::ListTableResponse *response) {
    GET_SNAPSHOT(response, request->catalog_version());
    Database *database = nullptr;
    CATALOG_REQUIRES_OK(response, snapshot->getDatabase(request->database_name(), database));
    const auto &names = database->listTable();
    *(response->mutable_table_names()) = {names.begin(), names.end()};
}

void CatalogController::getTable(const proto::GetTableRequest *request, proto::GetTableResponse *response) {
    GET_SNAPSHOT(response, request->catalog_version());
    Database *database = nullptr;
    CATALOG_REQUIRES_OK(response, snapshot->getDatabase(request->database_name(), database));
    if (request->ignore_not_found_error() && !database->hasTable(request->table_name())) {
        return;
    }
    Table *table = nullptr;
    CATALOG_REQUIRES_OK(response, database->getTable(request->table_name(), table));
    CATALOG_REQUIRES_OK(response, table->toProto(response->mutable_table(), request->detail_level()));
}

void CatalogController::createTable(const proto::CreateTableRequest *request, proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) { return catalog.createTable(request->table()); });
}

void CatalogController::dropTable(const proto::DropTableRequest *request, proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) {
        return catalog.dropTable({
            request->table_name(),
            request->database_name(),
            request->catalog_name(),
        });
    });
}

void CatalogController::updateTable(const proto::UpdateTableRequest *request, proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) { return catalog.updateTable(request->table()); });
}

void CatalogController::listTableRelatedTableGroup(const proto::ListTableRelatedTableGroupRequest *request,
                                                   proto::ListTableRelatedTableGroupResponse *response) {
    GET_SNAPSHOT(response, request->catalog_version());
    Database *database = nullptr;
    CATALOG_REQUIRES_OK(response, snapshot->getDatabase(request->database_name(), database));
    const auto &tableName = request->table_name();
    Table *table = nullptr;
    CATALOG_REQUIRES_OK(response, database->getTable(tableName, table));
    for (auto &it : database->tableGroups()) {
        const auto &loadStrategies = it.second->loadStrategies();
        if (loadStrategies.find(tableName) != loadStrategies.end()) {
            response->add_table_group_names(it.first);
        }
    }
}

void CatalogController::getTableStructure(const proto::GetTableStructureRequest *request,
                                          proto::GetTableStructureResponse *response) {
    GET_SNAPSHOT(response, request->catalog_version());
    Database *database = nullptr;
    CATALOG_REQUIRES_OK(response, snapshot->getDatabase(request->database_name(), database));
    Table *table = nullptr;
    CATALOG_REQUIRES_OK(response, database->getTable(request->table_name(), table));
    const auto &tableStructure = table->tableStructure();
    CATALOG_REQUIRES(response,
                     tableStructure != nullptr,
                     Status::NOT_FOUND,
                     "no table_structure for table:[",
                     request->database_name(),
                     ".",
                     request->table_name(),
                     "]");
    CATALOG_REQUIRES_OK(response,
                        tableStructure->toProto(response->mutable_table_structure(), request->detail_level()));
}

void CatalogController::updateTableStructure(const proto::UpdateTableStructureRequest *request,
                                             proto::CommonResponse *response) {
    execute(
        request, response, [&](Catalog &catalog) { return catalog.updateTableStructure(request->table_structure()); });
}

void CatalogController::addColumn(const proto::AddColumnRequest *request, proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) { return catalog.addColumn(*request); });
}

void CatalogController::updateColumn(const proto::UpdateColumnRequest *request, proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) { return catalog.updateColumn(*request); });
}

void CatalogController::dropColumn(const proto::DropColumnRequest *request, proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) { return catalog.dropColumn(*request); });
}

void CatalogController::createIndex(const proto::CreateIndexRequest *request, proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) { return catalog.createIndex(*request); });
}

void CatalogController::updateIndex(const proto::UpdateIndexRequest *request, proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) { return catalog.updateIndex(*request); });
}

void CatalogController::dropIndex(const proto::DropIndexRequest *request, proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) { return catalog.dropIndex(*request); });
}

void CatalogController::listPartition(const proto::ListPartitionRequest *request,
                                      proto::ListPartitionResponse *response) {
    GET_SNAPSHOT(response, request->catalog_version());
    Database *database = nullptr;
    CATALOG_REQUIRES_OK(response, snapshot->getDatabase(request->database_name(), database));
    auto partitionType = request->partition_type();
    if (partitionType == proto::PartitionType::TABLE_PARTITION) {
        Table *table = nullptr;
        CATALOG_REQUIRES_OK(response, database->getTable(request->table_name(), table));
        const auto &names = table->listPartition();
        *(response->mutable_partition_names()) = {names.begin(), names.end()};
    } else {
        CATALOG_REQUIRES(response,
                         false,
                         Status::INVALID_ARGUMENTS,
                         "unsupported partition_type:[",
                         proto::PartitionType_Code_Name(partitionType),
                         "]");
    }
}

void CatalogController::getPartition(const proto::GetPartitionRequest *request, proto::GetPartitionResponse *response) {
    GET_SNAPSHOT(response, request->catalog_version());
    Database *database = nullptr;
    CATALOG_REQUIRES_OK(response, snapshot->getDatabase(request->database_name(), database));
    auto partitionType = request->partition_type();
    if (partitionType == proto::PartitionType::TABLE_PARTITION) {
        Table *table = nullptr;
        CATALOG_REQUIRES_OK(response, database->getTable(request->table_name(), table));
        if (request->ignore_not_found_error() && !table->hasPartition(request->partition_name())) {
            return;
        }
        Partition *partition = nullptr;
        CATALOG_REQUIRES_OK(response, table->getPartition(request->partition_name(), partition));
        CATALOG_REQUIRES_OK(response, partition->toProto(response->mutable_partition(), request->detail_level()));
    } else {
        CATALOG_REQUIRES(response,
                         false,
                         Status::INVALID_ARGUMENTS,
                         "unsupported partition_type:[",
                         proto::PartitionType_Code_Name(partitionType),
                         "]");
    }
}

void CatalogController::createPartition(const proto::CreatePartitionRequest *request, proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) { return catalog.createPartition(*request); });
}

void CatalogController::dropPartition(const proto::DropPartitionRequest *request, proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) { return catalog.dropPartition(*request); });
}

void CatalogController::updatePartition(const proto::UpdatePartitionRequest *request, proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) { return catalog.updatePartition(*request); });
}

void CatalogController::updatePartitionTableStructure(const proto::UpdatePartitionTableStructureRequest *request,
                                                      proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) { return catalog.updatePartitionTableStructure(*request); });
}

void CatalogController::listTableGroup(const proto::ListTableGroupRequest *request,
                                       proto::ListTableGroupResponse *response) {
    GET_SNAPSHOT(response, request->catalog_version());
    Database *database = nullptr;
    CATALOG_REQUIRES_OK(response, snapshot->getDatabase(request->database_name(), database));
    const auto &names = database->listTableGroup();
    *(response->mutable_table_group_names()) = {names.begin(), names.end()};
}

void CatalogController::getTableGroup(const proto::GetTableGroupRequest *request,
                                      proto::GetTableGroupResponse *response) {
    GET_SNAPSHOT(response, request->catalog_version());
    Database *database = nullptr;
    CATALOG_REQUIRES_OK(response, snapshot->getDatabase(request->database_name(), database));
    if (request->ignore_not_found_error() && !database->hasTableGroup(request->table_group_name())) {
        return;
    }
    TableGroup *tableGroup = nullptr;
    CATALOG_REQUIRES_OK(response, database->getTableGroup(request->table_group_name(), tableGroup));
    CATALOG_REQUIRES_OK(response, tableGroup->toProto(response->mutable_table_group(), request->detail_level()));
}

void CatalogController::createTableGroup(const proto::CreateTableGroupRequest *request,
                                         proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) { return catalog.createTableGroup(request->table_group()); });
}

void CatalogController::dropTableGroup(const proto::DropTableGroupRequest *request, proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) {
        return catalog.dropTableGroup({
            request->table_group_name(),
            request->database_name(),
            request->catalog_name(),
        });
    });
}

void CatalogController::updateTableGroup(const proto::UpdateTableGroupRequest *request,
                                         proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) { return catalog.updateTableGroup(request->table_group()); });
}

void CatalogController::listLoadStrategy(const proto::ListLoadStrategyRequest *request,
                                         proto::ListLoadStrategyResponse *response) {
    GET_SNAPSHOT(response, request->catalog_version());
    Database *database = nullptr;
    CATALOG_REQUIRES_OK(response, snapshot->getDatabase(request->database_name(), database));
    TableGroup *tableGroup = nullptr;
    CATALOG_REQUIRES_OK(response, database->getTableGroup(request->table_group_name(), tableGroup));
    const auto &names = tableGroup->listLoadStrategy();
    *(response->mutable_table_names()) = {names.begin(), names.end()};
}

void CatalogController::getLoadStrategy(const proto::GetLoadStrategyRequest *request,
                                        proto::GetLoadStrategyResponse *response) {
    GET_SNAPSHOT(response, request->catalog_version());
    Database *database = nullptr;
    CATALOG_REQUIRES_OK(response, snapshot->getDatabase(request->database_name(), database));
    TableGroup *tableGroup = nullptr;
    CATALOG_REQUIRES_OK(response, database->getTableGroup(request->table_group_name(), tableGroup));
    if (request->ignore_not_found_error() && !tableGroup->hasLoadStrategy(request->table_name())) {
        return;
    }
    LoadStrategy *loadStrategy = nullptr;
    CATALOG_REQUIRES_OK(response, tableGroup->getLoadStrategy(request->table_name(), loadStrategy));
    CATALOG_REQUIRES_OK(response, loadStrategy->toProto(response->mutable_load_strategy(), request->detail_level()));
}

void CatalogController::createLoadStrategy(const proto::CreateLoadStrategyRequest *request,
                                           proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) { return catalog.createLoadStrategy(request->load_strategy()); });
}

void CatalogController::dropLoadStrategy(const proto::DropLoadStrategyRequest *request,
                                         proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) {
        return catalog.dropLoadStrategy({
            request->table_name(),
            request->table_group_name(),
            request->database_name(),
            request->catalog_name(),
        });
    });
}

void CatalogController::updateLoadStrategy(const proto::UpdateLoadStrategyRequest *request,
                                           proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) { return catalog.updateLoadStrategy(request->load_strategy()); });
}

void CatalogController::listFunction(const proto::ListFunctionRequest *request, proto::ListFunctionResponse *response) {
    GET_SNAPSHOT(response, request->catalog_version());
    Database *database = nullptr;
    CATALOG_REQUIRES_OK(response, snapshot->getDatabase(request->database_name(), database));
    const auto &names = database->listFunction();
    *(response->mutable_function_names()) = {names.begin(), names.end()};
}

void CatalogController::getFunction(const proto::GetFunctionRequest *request, proto::GetFunctionResponse *response) {
    GET_SNAPSHOT(response, request->catalog_version());
    Database *database = nullptr;
    CATALOG_REQUIRES_OK(response, snapshot->getDatabase(request->database_name(), database));
    if (request->ignore_not_found_error() && !database->hasFunction(request->function_name())) {
        return;
    }
    Function *function = nullptr;
    CATALOG_REQUIRES_OK(response, database->getFunction(request->function_name(), function));
    CATALOG_REQUIRES_OK(response, function->toProto(response->mutable_function(), request->detail_level()));
}

void CatalogController::createFunction(const proto::CreateFunctionRequest *request, proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) { return catalog.createFunction(request->function()); });
}

void CatalogController::dropFunction(const proto::DropFunctionRequest *request, proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) {
        return catalog.dropFunction({
            request->function_name(),
            request->database_name(),
            request->catalog_name(),
        });
    });
}

void CatalogController::updateFunction(const proto::UpdateFunctionRequest *request, proto::CommonResponse *response) {
    execute(request, response, [&](Catalog &catalog) { return catalog.updateFunction(request->function()); });
}

void CatalogController::listBuild(const proto::ListBuildRequest *request, proto::ListBuildResponse *response) {
    CATALOG_REQUIRES(
        response, !request->catalog_name().empty(), Status::INVALID_ARGUMENTS, "catalog name is not specified");
    proto::BuildId targetBuildId;
    targetBuildId.set_catalog_name(request->catalog_name());
    targetBuildId.set_database_name(request->database_name());
    targetBuildId.set_table_name(request->table_name());
    targetBuildId.set_partition_name(request->partition_name());
    {
        autil::ScopedReadLock lock(_buildLock);
        for (const auto &build : _builds) {
            if (isBuildIdMatch(targetBuildId, build.build_id())) {
                *response->add_build_ids() = build.build_id();
                *response->add_build_targets() = build.target();
            }
        }
    }
}
void CatalogController::getBuild(const proto::GetBuildRequest *request, proto::GetBuildResponse *response) {
    CATALOG_REQUIRES(response,
                     !request->build_id().catalog_name().empty(),
                     Status::INVALID_ARGUMENTS,
                     "catalog name is not specified in build id");
    {
        autil::ScopedReadLock lock(_buildLock);
        for (const auto &build : _builds) {
            if (isBuildIdMatch(request->build_id(), build.build_id())) {
                *response->add_builds() = build;
            }
        }
    }
    if (!request->build_id().partition_name().empty()) {
        CATALOG_REQUIRES(response,
                         response->builds_size() > 0,
                         Status::NOT_FOUND,
                         "build id [",
                         request->build_id().ShortDebugString(),
                         "] not found");
    }
}
void CatalogController::createBuild(const proto::CreateBuildRequest *request, proto::CreateBuildResponse *response) {
    CATALOG_REQUIRES(response, request->has_partition(), Status::INVALID_ARGUMENTS, "partition is missing");
    CATALOG_REQUIRES(response, !request->store_root().empty(), Status::INVALID_ARGUMENTS, "store root is empty");
    proto::Build newBuild;
    CATALOG_REQUIRES_OK(response, createBuild(request->partition(), request->store_root(), &newBuild));
    CATALOG_REQUIRES_OK(response, checkBuildId(newBuild));
    {
        autil::ScopedWriteLock lock(_buildLock);
        auto builds = _builds;
        for (const auto &build : builds) {
            CATALOG_REQUIRES(response,
                             !ProtoUtil::compareProto(build.build_id(), newBuild.build_id()),
                             Status::INTERNAL_ERROR,
                             "build id [",
                             newBuild.build_id().ShortDebugString(),
                             "] already exist, try it later after 1s");
        }

        builds.push_back(newBuild);
        auto status = _writer->write(builds);
        if (isOk(status)) {
            _builds.swap(builds);
        }
        CATALOG_REQUIRES_OK(response, status);
    }
    *response->mutable_build_id() = newBuild.build_id();
}
void CatalogController::dropBuild(const proto::DropBuildRequest *request, proto::BuildCommonResponse *response) {
    CATALOG_REQUIRES(response,
                     !request->build_id().catalog_name().empty(),
                     Status::INVALID_ARGUMENTS,
                     "catalog name is not specified in build id");
    std::vector<proto::Build> builds;
    {
        autil::ScopedWriteLock lock(_buildLock);
        for (const auto &build : _builds) {
            if (!isBuildIdMatch(request->build_id(), build.build_id())) {
                builds.push_back(build);
            }
        }
        auto status = _writer->write(builds);
        if (isOk(status)) {
            _builds.swap(builds);
        }
        CATALOG_REQUIRES_OK(response, status);
    }
}
void CatalogController::updateBuild(const proto::UpdateBuildRequest *request, proto::BuildCommonResponse *response) {
    const auto &newBuild = request->build();
    CATALOG_REQUIRES_OK(response, checkBuildId(newBuild));
    {
        autil::ScopedWriteLock lock(_buildLock);
        auto builds = _builds;
        for (auto &build : builds) {
            if (!ProtoUtil::compareProto(request->build().build_id(), build.build_id())) {
                continue;
            }
            if (request->build().has_target()) {
                const auto &requestTarget = request->build().target();
                auto target = build.mutable_target();
                if (requestTarget.type() != proto::BuildTarget::NONE) {
                    CATALOG_REQUIRES(response,
                                     target->type() == requestTarget.type(),
                                     Status::INVALID_ARGUMENTS,
                                     "type of build cannot be modified, [",
                                     target->type(),
                                     "] to [",
                                     requestTarget.type(),
                                     "]");
                }
                if (requestTarget.build_state() != proto::BuildState::NONE) {
                    target->set_build_state(requestTarget.build_state());
                }
                if (!requestTarget.config_path().empty()) {
                    target->set_config_path(requestTarget.config_path());
                }
            }
            if (request->build().has_current()) {
                const auto &requestCurrent = request->build().current();
                auto current = build.mutable_current();
                if (requestCurrent.build_state() != proto::BuildState::NONE) {
                    current->set_build_state(requestCurrent.build_state());
                }
                if (!requestCurrent.config_path().empty()) {
                    current->set_config_path(requestCurrent.config_path());
                }
                if (!requestCurrent.last_error().empty()) {
                    current->set_last_error(requestCurrent.last_error());
                }
                if (!requestCurrent.index_root().empty()) {
                    current->set_index_root(requestCurrent.index_root());
                }
                if (requestCurrent.shards_size() > 0) {
                    current->clear_shards();
                    for (int i = 0; i < requestCurrent.shards_size(); ++i) {
                        *current->add_shards() = requestCurrent.shards(i);
                    }
                }
            }
            break;
        }
        auto status = _writer->write(builds);
        if (isOk(status)) {
            _builds.swap(builds);
        }
        CATALOG_REQUIRES_OK(response, status);
    }
}

template <typename T>
void CatalogController::execute(const T *request, proto::CommonResponse *response, UpdateFunction doUpdate) {
    auto status = checkAndUpdate(
        [&](CatalogSnapshot *snapshot) {
            CATALOG_CHECK(snapshot != nullptr, Status::INTERNAL_ERROR, "old snapshot not exists");
            auto inputVersion = request->catalog_version();
            auto currentVersion = snapshot->version();
            CATALOG_CHECK(currentVersion == inputVersion,
                          Status::VERSION_EXPIRED,
                          "version:[",
                          inputVersion,
                          "] for catalog:[",
                          snapshot->catalogName(),
                          "] expired, expected version:[",
                          currentVersion,
                          "]");
            return StatusBuilder::ok();
        },
        std::move(doUpdate),
        [&](CatalogSnapshot *oldSnapshot, CatalogSnapshot *newSnapshot) {
            AUTIL_LOG(INFO, "catalog:[%s] installing new version:[%ld]", _catalogName.c_str(), newSnapshot->version());
            CATALOG_CHECK_OK(_writer->write(oldSnapshot, newSnapshot));
            AUTIL_LOG(INFO, "catalog:[%s] new version:[%ld] installed", _catalogName.c_str(), newSnapshot->version());
            return StatusBuilder::ok();
        },
        [&](CatalogSnapshot *oldSnapshot, CatalogSnapshot *newSnapshot) {
            response->set_source_catalog_version(oldSnapshot->version());
            response->set_target_catalog_version(newSnapshot->version());
            return StatusBuilder::ok();
        });
    CATALOG_REQUIRES_OK(response, status);
}

Status CatalogController::checkAndUpdate(CheckFunction doCheck,
                                         UpdateFunction doUpdate,
                                         WriteFunction doWrite,
                                         FinishFunction doFinish) {
    autil::ScopedWriteLock lock(_lock);
    // check
    CATALOG_CHECK_OK(doCheck(_snapshot.get()));

    // create or copy
    auto catalog = _snapshot == nullptr ? std::make_unique<Catalog>() : _snapshot->clone();
    CATALOG_CHECK(catalog != nullptr, Status::INTERNAL_ERROR, "failed to clone snapshot");

    // update
    CATALOG_CHECK_OK(doUpdate(*catalog));
    CATALOG_CHECK_OK(alignVersion(_snapshot.get(), *catalog));

    // write
    CATALOG_CHECK_OK(doWrite(_snapshot.get(), catalog.get()));

    // finish
    CATALOG_CHECK_OK(doFinish(_snapshot.get(), catalog.get()));
    _snapshot = std::move(catalog);
    return StatusBuilder::ok();
}

Status CatalogController::alignVersion(CatalogSnapshot *oldSnapshot, Catalog &newSnapshot) {
    CatalogVersion nextVersion = kInitCatalogVersion;
    if (oldSnapshot == nullptr) {
        CatalogVersion latestVersion = kInvalidCatalogVersion;
        CATALOG_CHECK_OK(_reader->getLatestVersion(latestVersion));
        if (latestVersion == kInvalidCatalogVersion) {
            nextVersion = kInitCatalogVersion;
        } else {
            nextVersion = latestVersion + 1;
        }
    } else {
        nextVersion = oldSnapshot->version() + 1;
    }

    newSnapshot.alignVersion(nextVersion);
    return StatusBuilder::ok();
}

std::shared_ptr<CatalogSnapshot> CatalogController::getSnapshot(std::shared_ptr<CatalogSnapshot> currentSnapshot,
                                                                CatalogVersion targetVersion) {
    if (targetVersion <= 0 || targetVersion == currentSnapshot->version()) {
        return currentSnapshot;
    }
    // TODO(chekong.ygm): 后续可以考虑引入snapshot cache
    return retrieveSnapshot(targetVersion);
}

std::unique_ptr<CatalogSnapshot> CatalogController::retrieveSnapshot(CatalogVersion version) {
    proto::Catalog proto;
    const auto &s1 = _reader->read(version, &proto);
    if (!isOk(s1)) {
        AUTIL_LOG(ERROR,
                  "failed to read catalog:[%s] with version:[%ld] and error:[%s]",
                  _catalogName.c_str(),
                  version,
                  s1.message().c_str());
        return nullptr;
    }
    auto catalog = std::make_unique<Catalog>();
    const auto &s2 = catalog->fromProto(proto);
    if (!isOk(s2)) {
        AUTIL_LOG(ERROR,
                  "failed to parse catalog:[%s] with version:[%ld] and error:[%s]",
                  _catalogName.c_str(),
                  version,
                  s2.message().c_str());
        return nullptr;
    }
    return catalog;
}

Status CatalogController::checkBuildId(const proto::Build &build) {
    CATALOG_CHECK(build.has_build_id(), Status::INVALID_ARGUMENTS, "build id not specified");
    const auto &buildId = build.build_id();
    CATALOG_CHECK(buildId.generation_id() > 0,
                  Status::INVALID_ARGUMENTS,
                  "invalid generation id [",
                  buildId.generation_id(),
                  "] in build id");
    CATALOG_CHECK(!buildId.partition_name().empty(), Status::INVALID_ARGUMENTS, "partition name is empty in build id");
    CATALOG_CHECK(!buildId.table_name().empty(), Status::INVALID_ARGUMENTS, "table name is empty in build id");
    CATALOG_CHECK(!buildId.database_name().empty(), Status::INVALID_ARGUMENTS, "database name is empty in build id");
    CATALOG_CHECK(!buildId.catalog_name().empty(), Status::INVALID_ARGUMENTS, "catalog name is empty in build id");
    return StatusBuilder::ok();
}

bool CatalogController::isBuildIdMatch(const proto::BuildId &targetBuildId, const proto::BuildId &buildId) {
    if (targetBuildId.database_name().empty()) {
        return buildId.catalog_name() == targetBuildId.catalog_name();
    }
    if (targetBuildId.table_name().empty()) {
        return buildId.catalog_name() == targetBuildId.catalog_name() &&
               buildId.database_name() == targetBuildId.database_name();
    }
    if (targetBuildId.partition_name().empty()) {
        return buildId.catalog_name() == targetBuildId.catalog_name() &&
               buildId.database_name() == targetBuildId.database_name() &&
               buildId.table_name() == targetBuildId.table_name();
    }
    if (targetBuildId.generation_id() == 0) {
        return buildId.catalog_name() == targetBuildId.catalog_name() &&
               buildId.database_name() == targetBuildId.database_name() &&
               buildId.table_name() == targetBuildId.table_name() &&
               buildId.partition_name() == targetBuildId.partition_name();
    }
    return buildId.catalog_name() == targetBuildId.catalog_name() &&
           buildId.database_name() == targetBuildId.database_name() &&
           buildId.table_name() == targetBuildId.table_name() &&
           buildId.partition_name() == targetBuildId.partition_name() &&
           buildId.generation_id() == targetBuildId.generation_id();
}

Status CatalogController::createBuild(const proto::Partition &part, const std::string &storeRoot, proto::Build *build) {
    Partition partition;
    CATALOG_CHECK_OK(partition.fromProto(part));
    const auto &partitionId = partition.id();
    auto buildId = build->mutable_build_id();
    buildId->set_generation_id(autil::TimeUtility::currentTimeInSeconds());
    buildId->set_partition_name(partitionId.partitionName);
    buildId->set_table_name(partitionId.tableName);
    buildId->set_database_name(partitionId.databaseName);
    buildId->set_catalog_name(partitionId.catalogName);
    auto buildType = partition.getTableStructure()->tableStructureConfig().build_type();
    CATALOG_CHECK(buildType == proto::BuildType::OFFLINE,
                  Status::INVALID_ARGUMENTS,
                  "build_type [",
                  buildType,
                  "] is not OFFLINE, do not create build");
    auto target = build->mutable_target();
    target->set_type(proto::BuildTarget::BATCH_BUILD);
    target->set_build_state(proto::BuildState::RUNNING);
    std::string templatePath;
    CATALOG_CHECK_OK(getBSTemplateConfigPath(&templatePath));
    std::string configPath;
    CATALOG_CHECK_OK(BSConfigMaker::Make(partition, templatePath, storeRoot, &configPath));
    target->set_config_path(configPath);
    return StatusBuilder::ok();
}

Status CatalogController::getBSTemplateConfigPath(std::string *templatePath) {
    *templatePath = autil::EnvUtil::getEnv("BS_TEMPLATE_CONFIG_PATH", std::string("/usr/local/etc/template"));
    CATALOG_CHECK(fslib::fs::FileSystem::isExist(*templatePath) == fslib::EC_TRUE,
                  Status::INTERNAL_ERROR,
                  "bs template config path [",
                  *templatePath,
                  "] not exist");
    return StatusBuilder::ok();
}

Status CatalogController::getCatalog(Catalog *catalog) {
    autil::ScopedReadLock lock(_lock);
    proto::Catalog proto;
    CATALOG_CHECK(_snapshot != nullptr, Status::INTERNAL_ERROR, "catalog snapshot is nullptr");
    auto s = _snapshot->toProto(&proto);
    if (!isOk(s)) {
        return s;
    }
    catalog->fromProto(proto);
    return StatusBuilder::ok();
}

std::map<PartitionId, proto::Build> CatalogController::getCurrentBuilds() {
    autil::ScopedReadLock lock(_buildLock);

    std::map<PartitionId, proto::Build> buildMap;

    for (const auto &build : _builds) {
        const auto &buildId = build.build_id();
        PartitionId id = {buildId.partition_name(),
                          buildId.table_name(),
                          buildId.database_name(),
                          buildId.catalog_name(),
                          proto::PartitionType::TABLE_PARTITION};
        buildMap.emplace(id, build);
    }

    return buildMap;
}

} // namespace catalog
