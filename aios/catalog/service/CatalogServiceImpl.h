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
#include <map>
#include <string>

#include "autil/Log.h"
#include "catalog/proto/CatalogService.pb.h"
#include "catalog/service/CatalogControllerManager.h"
#include "catalog/store/IStore.h"

namespace catalog {

class CatalogServiceImpl : public proto::CatalogService {
public:
    explicit CatalogServiceImpl(const std::string &storeUri);
    ~CatalogServiceImpl();
    CatalogServiceImpl(const CatalogServiceImpl &) = delete;
    CatalogServiceImpl &operator=(const CatalogServiceImpl &) = delete;

public:
    bool start();
    void stop();

public:
    // ---------- API for Catalog ---------------
    void listCatalog(::google::protobuf::RpcController *controller,
                     const proto::ListCatalogRequest *request,
                     proto::ListCatalogResponse *response,
                     ::google::protobuf::Closure *done) override;
    void getCatalog(::google::protobuf::RpcController *controller,
                    const proto::GetCatalogRequest *request,
                    proto::GetCatalogResponse *response,
                    ::google::protobuf::Closure *done) override;
    void createCatalog(::google::protobuf::RpcController *controller,
                       const proto::CreateCatalogRequest *request,
                       proto::CommonResponse *response,
                       ::google::protobuf::Closure *done) override;
    void dropCatalog(::google::protobuf::RpcController *controller,
                     const proto::DropCatalogRequest *request,
                     proto::CommonResponse *response,
                     ::google::protobuf::Closure *done) override;
    void updateCatalog(::google::protobuf::RpcController *controller,
                       const proto::UpdateCatalogRequest *request,
                       proto::CommonResponse *response,
                       ::google::protobuf::Closure *done) override;
    void updateCatalogStatus(::google::protobuf::RpcController *controller,
                             const proto::UpdateCatalogStatusRequest *request,
                             proto::CommonResponse *response,
                             ::google::protobuf::Closure *done) override;

    // ---------- API for Database ---------------
    void listDatabase(::google::protobuf::RpcController *controller,
                      const proto::ListDatabaseRequest *request,
                      proto::ListDatabaseResponse *response,
                      ::google::protobuf::Closure *done) override;
    void getDatabase(::google::protobuf::RpcController *controller,
                     const proto::GetDatabaseRequest *request,
                     proto::GetDatabaseResponse *response,
                     ::google::protobuf::Closure *done) override;
    void createDatabase(::google::protobuf::RpcController *controller,
                        const proto::CreateDatabaseRequest *request,
                        proto::CommonResponse *response,
                        ::google::protobuf::Closure *done) override;
    void dropDatabase(::google::protobuf::RpcController *controller,
                      const proto::DropDatabaseRequest *request,
                      proto::CommonResponse *response,
                      ::google::protobuf::Closure *done) override;
    void updateDatabase(::google::protobuf::RpcController *controller,
                        const proto::UpdateDatabaseRequest *request,
                        proto::CommonResponse *response,
                        ::google::protobuf::Closure *done) override;

    // ---------- API for Table ------------------

    void listTable(::google::protobuf::RpcController *controller,
                   const proto::ListTableRequest *request,
                   proto::ListTableResponse *response,
                   ::google::protobuf::Closure *done) override;
    void getTable(::google::protobuf::RpcController *controller,
                  const proto::GetTableRequest *request,
                  proto::GetTableResponse *response,
                  ::google::protobuf::Closure *done) override;
    void createTable(::google::protobuf::RpcController *controller,
                     const proto::CreateTableRequest *request,
                     proto::CommonResponse *response,
                     ::google::protobuf::Closure *done) override;
    void dropTable(::google::protobuf::RpcController *controller,
                   const proto::DropTableRequest *request,
                   proto::CommonResponse *response,
                   ::google::protobuf::Closure *done) override;
    void updateTable(::google::protobuf::RpcController *controller,
                     const proto::UpdateTableRequest *request,
                     proto::CommonResponse *response,
                     ::google::protobuf::Closure *done) override;
    void listTableRelatedTableGroup(::google::protobuf::RpcController *controller,
                                    const proto::ListTableRelatedTableGroupRequest *request,
                                    proto::ListTableRelatedTableGroupResponse *response,
                                    ::google::protobuf::Closure *done) override;

    // ---------- API for TableStructure ---------
    void getTableStructure(::google::protobuf::RpcController *controller,
                           const proto::GetTableStructureRequest *request,
                           proto::GetTableStructureResponse *response,
                           ::google::protobuf::Closure *done) override;
    void updateTableStructure(::google::protobuf::RpcController *controller,
                              const proto::UpdateTableStructureRequest *request,
                              proto::CommonResponse *response,
                              ::google::protobuf::Closure *done) override;
    void addColumn(::google::protobuf::RpcController *controller,
                   const proto::AddColumnRequest *request,
                   proto::CommonResponse *response,
                   ::google::protobuf::Closure *done) override;
    void updateColumn(::google::protobuf::RpcController *controller,
                      const proto::UpdateColumnRequest *request,
                      proto::CommonResponse *response,
                      ::google::protobuf::Closure *done) override;
    void dropColumn(::google::protobuf::RpcController *controller,
                    const proto::DropColumnRequest *request,
                    proto::CommonResponse *response,
                    ::google::protobuf::Closure *done) override;
    void createIndex(::google::protobuf::RpcController *controller,
                     const proto::CreateIndexRequest *request,
                     proto::CommonResponse *response,
                     ::google::protobuf::Closure *done) override;
    void updateIndex(::google::protobuf::RpcController *controller,
                     const proto::UpdateIndexRequest *request,
                     proto::CommonResponse *response,
                     ::google::protobuf::Closure *done) override;
    void dropIndex(::google::protobuf::RpcController *controller,
                   const proto::DropIndexRequest *request,
                   proto::CommonResponse *response,
                   ::google::protobuf::Closure *done) override;

    // ---------- API for Partition --------------
    void listPartition(::google::protobuf::RpcController *controller,
                       const proto::ListPartitionRequest *request,
                       proto::ListPartitionResponse *response,
                       ::google::protobuf::Closure *done) override;
    void getPartition(::google::protobuf::RpcController *controller,
                      const proto::GetPartitionRequest *request,
                      proto::GetPartitionResponse *response,
                      ::google::protobuf::Closure *done) override;
    void createPartition(::google::protobuf::RpcController *controller,
                         const proto::CreatePartitionRequest *request,
                         proto::CommonResponse *response,
                         ::google::protobuf::Closure *done) override;
    void dropPartition(::google::protobuf::RpcController *controller,
                       const proto::DropPartitionRequest *request,
                       proto::CommonResponse *response,
                       ::google::protobuf::Closure *done) override;
    void updatePartition(::google::protobuf::RpcController *controller,
                         const proto::UpdatePartitionRequest *request,
                         proto::CommonResponse *response,
                         ::google::protobuf::Closure *done) override;
    void updatePartitionTableStructure(::google::protobuf::RpcController *controller,
                                       const proto::UpdatePartitionTableStructureRequest *request,
                                       proto::CommonResponse *response,
                                       ::google::protobuf::Closure *done) override;

    // ---------- API for TableGroup -------------
    void listTableGroup(::google::protobuf::RpcController *controller,
                        const proto::ListTableGroupRequest *request,
                        proto::ListTableGroupResponse *response,
                        ::google::protobuf::Closure *done) override;
    void getTableGroup(::google::protobuf::RpcController *controller,
                       const proto::GetTableGroupRequest *request,
                       proto::GetTableGroupResponse *response,
                       ::google::protobuf::Closure *done) override;
    void createTableGroup(::google::protobuf::RpcController *controller,
                          const proto::CreateTableGroupRequest *request,
                          proto::CommonResponse *response,
                          ::google::protobuf::Closure *done) override;
    void dropTableGroup(::google::protobuf::RpcController *controller,
                        const proto::DropTableGroupRequest *request,
                        proto::CommonResponse *response,
                        ::google::protobuf::Closure *done) override;
    void updateTableGroup(::google::protobuf::RpcController *controller,
                          const proto::UpdateTableGroupRequest *request,
                          proto::CommonResponse *response,
                          ::google::protobuf::Closure *done) override;

    // ---------- API for LoadStrategy -----------
    void listLoadStrategy(::google::protobuf::RpcController *controller,
                          const proto::ListLoadStrategyRequest *request,
                          proto::ListLoadStrategyResponse *response,
                          ::google::protobuf::Closure *done) override;
    void getLoadStrategy(::google::protobuf::RpcController *controller,
                         const proto::GetLoadStrategyRequest *request,
                         proto::GetLoadStrategyResponse *response,
                         ::google::protobuf::Closure *done) override;
    void createLoadStrategy(::google::protobuf::RpcController *controller,
                            const proto::CreateLoadStrategyRequest *request,
                            proto::CommonResponse *response,
                            ::google::protobuf::Closure *done) override;
    void dropLoadStrategy(::google::protobuf::RpcController *controller,
                          const proto::DropLoadStrategyRequest *request,
                          proto::CommonResponse *response,
                          ::google::protobuf::Closure *done) override;
    void updateLoadStrategy(::google::protobuf::RpcController *controller,
                            const proto::UpdateLoadStrategyRequest *request,
                            proto::CommonResponse *response,
                            ::google::protobuf::Closure *done) override;

    // ---------- API for Function ---------------
    void listFunction(::google::protobuf::RpcController *controller,
                      const proto::ListFunctionRequest *request,
                      proto::ListFunctionResponse *response,
                      ::google::protobuf::Closure *done) override;
    void getFunction(::google::protobuf::RpcController *controller,
                     const proto::GetFunctionRequest *request,
                     proto::GetFunctionResponse *response,
                     ::google::protobuf::Closure *done) override;
    void createFunction(::google::protobuf::RpcController *controller,
                        const proto::CreateFunctionRequest *request,
                        proto::CommonResponse *response,
                        ::google::protobuf::Closure *done) override;
    void dropFunction(::google::protobuf::RpcController *controller,
                      const proto::DropFunctionRequest *request,
                      proto::CommonResponse *response,
                      ::google::protobuf::Closure *done) override;
    void updateFunction(::google::protobuf::RpcController *controller,
                        const proto::UpdateFunctionRequest *request,
                        proto::CommonResponse *response,
                        ::google::protobuf::Closure *done) override;

    // ---------- API for Build ---------------
    void listBuild(::google::protobuf::RpcController *controller,
                   const proto::ListBuildRequest *request,
                   proto::ListBuildResponse *response,
                   ::google::protobuf::Closure *done) override;
    void getBuild(::google::protobuf::RpcController *controller,
                  const proto::GetBuildRequest *request,
                  proto::GetBuildResponse *response,
                  ::google::protobuf::Closure *done) override;
    void createBuild(::google::protobuf::RpcController *controller,
                     const proto::CreateBuildRequest *request,
                     proto::CreateBuildResponse *response,
                     ::google::protobuf::Closure *done) override;
    void dropBuild(::google::protobuf::RpcController *controller,
                   const proto::DropBuildRequest *request,
                   proto::BuildCommonResponse *response,
                   ::google::protobuf::Closure *done) override;
    void updateBuild(::google::protobuf::RpcController *controller,
                     const proto::UpdateBuildRequest *request,
                     proto::BuildCommonResponse *response,
                     ::google::protobuf::Closure *done) override;

    // for suez admin
    std::shared_ptr<CatalogControllerManager> getManager();

private:
    std::atomic<bool> _serviceReady;
    std::string _storeUri;
    std::shared_ptr<IStore> _store;
    std::shared_ptr<CatalogControllerManager> _controllerManager;
    AUTIL_LOG_DECLARE();
};

} // namespace catalog
