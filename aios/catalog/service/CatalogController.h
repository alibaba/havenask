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

#include <atomic>
#include <functional>
#include <string>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/LoopThread.h"
#include "catalog/entity/CatalogSnapshot.h"
#include "catalog/proto/CatalogService.pb.h"
#include "catalog/store/ICatalogReader.h"
#include "catalog/store/ICatalogWriter.h"
#include "catalog/store/IStore.h"
#include "catalog/util/StatusBuilder.h"

namespace catalog {

class CatalogController {
public:
    CatalogController(const std::string &catalogName, const std::shared_ptr<IStore> &store);
    virtual ~CatalogController();
    CatalogController(const CatalogController &) = delete;
    CatalogController &operator=(const CatalogController &) = delete;

public:
    bool init();
    bool recover();

    // ---------- API for Catalog ----------------
    const proto::EntityStatus &status();
    void getCatalog(const proto::GetCatalogRequest *request, proto::GetCatalogResponse *response);
    void createCatalog(const proto::CreateCatalogRequest *request, proto::CommonResponse *response);
    void dropCatalog(const proto::DropCatalogRequest *request, proto::CommonResponse *response);
    void updateCatalog(const proto::UpdateCatalogRequest *request, proto::CommonResponse *response);
    void updateCatalogStatus(const proto::UpdateCatalogStatusRequest *request, proto::CommonResponse *response);

    // ---------- API for Database ---------------
    void listDatabase(const proto::ListDatabaseRequest *request, proto::ListDatabaseResponse *response);
    void getDatabase(const proto::GetDatabaseRequest *request, proto::GetDatabaseResponse *response);
    void createDatabase(const proto::CreateDatabaseRequest *request, proto::CommonResponse *response);
    void dropDatabase(const proto::DropDatabaseRequest *request, proto::CommonResponse *response);
    void updateDatabase(const proto::UpdateDatabaseRequest *request, proto::CommonResponse *response);

    // ---------- API for Table ------------------
    void listTable(const proto::ListTableRequest *request, proto::ListTableResponse *response);
    void getTable(const proto::GetTableRequest *request, proto::GetTableResponse *response);
    void createTable(const proto::CreateTableRequest *request, proto::CommonResponse *response);
    void dropTable(const proto::DropTableRequest *request, proto::CommonResponse *response);
    void updateTable(const proto::UpdateTableRequest *request, proto::CommonResponse *response);
    void listTableRelatedTableGroup(const proto::ListTableRelatedTableGroupRequest *request,
                                    proto::ListTableRelatedTableGroupResponse *response);

    // ---------- API for TableStructure ---------
    void getTableStructure(const proto::GetTableStructureRequest *request, proto::GetTableStructureResponse *response);
    void updateTableStructure(const proto::UpdateTableStructureRequest *request, proto::CommonResponse *response);
    void addColumn(const proto::AddColumnRequest *request, proto::CommonResponse *response);
    void updateColumn(const proto::UpdateColumnRequest *request, proto::CommonResponse *response);
    void dropColumn(const proto::DropColumnRequest *request, proto::CommonResponse *response);
    void createIndex(const proto::CreateIndexRequest *request, proto::CommonResponse *response);
    void updateIndex(const proto::UpdateIndexRequest *request, proto::CommonResponse *response);
    void dropIndex(const proto::DropIndexRequest *request, proto::CommonResponse *response);

    // ---------- API for Partition --------------
    void listPartition(const proto::ListPartitionRequest *request, proto::ListPartitionResponse *response);
    void getPartition(const proto::GetPartitionRequest *request, proto::GetPartitionResponse *response);
    void createPartition(const proto::CreatePartitionRequest *request, proto::CommonResponse *response);
    void dropPartition(const proto::DropPartitionRequest *request, proto::CommonResponse *response);
    void updatePartition(const proto::UpdatePartitionRequest *request, proto::CommonResponse *response);
    void updatePartitionTableStructure(const proto::UpdatePartitionTableStructureRequest *request,
                                       proto::CommonResponse *response);

    // ---------- API for TableGroup -------------
    void listTableGroup(const proto::ListTableGroupRequest *request, proto::ListTableGroupResponse *response);
    void getTableGroup(const proto::GetTableGroupRequest *request, proto::GetTableGroupResponse *response);
    void createTableGroup(const proto::CreateTableGroupRequest *request, proto::CommonResponse *response);
    void dropTableGroup(const proto::DropTableGroupRequest *request, proto::CommonResponse *response);
    void updateTableGroup(const proto::UpdateTableGroupRequest *request, proto::CommonResponse *response);

    // ---------- API for LoadStrategy -----------
    void listLoadStrategy(const proto::ListLoadStrategyRequest *request, proto::ListLoadStrategyResponse *response);
    void getLoadStrategy(const proto::GetLoadStrategyRequest *request, proto::GetLoadStrategyResponse *response);
    void createLoadStrategy(const proto::CreateLoadStrategyRequest *request, proto::CommonResponse *response);
    void dropLoadStrategy(const proto::DropLoadStrategyRequest *request, proto::CommonResponse *response);
    void updateLoadStrategy(const proto::UpdateLoadStrategyRequest *request, proto::CommonResponse *response);

    // ---------- API for Function ---------------
    void listFunction(const proto::ListFunctionRequest *request, proto::ListFunctionResponse *response);
    void getFunction(const proto::GetFunctionRequest *request, proto::GetFunctionResponse *response);
    void createFunction(const proto::CreateFunctionRequest *request, proto::CommonResponse *response);
    void dropFunction(const proto::DropFunctionRequest *request, proto::CommonResponse *response);
    void updateFunction(const proto::UpdateFunctionRequest *request, proto::CommonResponse *response);

    // ---------- API for Build ---------------
    void listBuild(const proto::ListBuildRequest *request, proto::ListBuildResponse *response);
    void getBuild(const proto::GetBuildRequest *request, proto::GetBuildResponse *response);
    void createBuild(const proto::CreateBuildRequest *request, proto::CreateBuildResponse *response);
    void dropBuild(const proto::DropBuildRequest *request, proto::BuildCommonResponse *response);
    void updateBuild(const proto::UpdateBuildRequest *request, proto::BuildCommonResponse *response);

    // ----------- API for TargetGenerator ----------
    Status getCatalog(Catalog *catalog);
    std::map<PartitionId, proto::Build> getCurrentBuilds();

private:
    using CheckFunction = std::function<Status(CatalogSnapshot *)>;
    using UpdateFunction = std::function<Status(Catalog &)>;
    using WriteFunction = std::function<Status(CatalogSnapshot *, CatalogSnapshot *)>;
    using FinishFunction = std::function<Status(CatalogSnapshot *, CatalogSnapshot *)>;
    template <typename T>
    void execute(const T *request, proto::CommonResponse *response, UpdateFunction doUpdate);
    Status
    checkAndUpdate(CheckFunction doCheck, UpdateFunction doUpdate, WriteFunction doWrite, FinishFunction doFinish);
    Status alignVersion(CatalogSnapshot *oldSnapshot, Catalog &newSnapshot);
    std::shared_ptr<CatalogSnapshot> getSnapshot(std::shared_ptr<CatalogSnapshot> currentSnapshot,
                                                 CatalogVersion targetVersion);
    std::unique_ptr<CatalogSnapshot> retrieveSnapshot(CatalogVersion version);

    Status checkBuildId(const proto::Build &build);
    bool isBuildIdMatch(const proto::BuildId &targetBuildId, const proto::BuildId &buildId);
    Status createBuild(const proto::Partition &partition, const std::string &storeRoot, proto::Build *build);

    void syncLoop();
    Status getCurrentPartitions(const Catalog *catalog, std::map<PartitionId, const Partition *> &partitions);
    Status getCurrentBuildIds(const std::string &catalogName, std::map<PartitionId, uint32_t> &buildIds);
    std::string getStoreRoot(const std::unique_ptr<Catalog> &catalog, const PartitionId &id);
    void createBuild(const Partition *partition, const std::string &storeRoot);
    void updateBuild(const Partition *partition, const std::string &storeRoot, uint32_t generationId);
    void dropBuild(const PartitionId &partitionId);

private:
    // virtual for test
    virtual Status getBSTemplateConfigPath(std::string *templatePath);

private:
    std::string _catalogName;
    autil::ReadWriteLock _lock;
    std::shared_ptr<CatalogSnapshot> _snapshot AUTIL_GUARDED_BY(_lock);
    std::shared_ptr<IStore> _store;
    std::unique_ptr<ICatalogReader> _reader;
    std::unique_ptr<ICatalogWriter> _writer;

    // build
    autil::ReadWriteLock _buildLock;
    std::vector<proto::Build> _builds AUTIL_GUARDED_BY(_buildLock);

    std::unique_ptr<Catalog> _lastCatalog; // for life cycle of _lastPartitions
    std::map<PartitionId, const Partition *> _lastPartitions;
    autil::LoopThreadPtr _syncThread; // sync change of partition to build

private:
    AUTIL_LOG_DECLARE();
};

using CatalogControllerPtr = std::shared_ptr<CatalogController>;

} // namespace catalog
