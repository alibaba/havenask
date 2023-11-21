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
#include "catalog/service/CatalogServiceImpl.h"

#include <memory>

#include "catalog/service/CatalogAccessLog.h"
#include "catalog/store/StoreFactory.h"
#include "catalog/util/StatusBuilder.h"

namespace catalog {
AUTIL_LOG_SETUP(catalog, CatalogServiceImpl);

template <typename T>
class ServiceRpcGuard {
public:
    explicit ServiceRpcGuard(::google::protobuf::Closure *done, T *response) : _done(done), _response(response) {
        // TODO(chekong.ygm): 指标汇报
    }
    ~ServiceRpcGuard() {
        if (_response != nullptr && !_response->has_status()) {
            _response->mutable_status()->set_code(Status::OK);
        }
        if (_done != nullptr) {
            _done->Run();
        }
    }
    ServiceRpcGuard(const ServiceRpcGuard &) = delete;
    ServiceRpcGuard &operator=(const ServiceRpcGuard &) = delete;
    ServiceRpcGuard(ServiceRpcGuard &&) = default;

public:
    ::google::protobuf::Closure *_done;
    T *_response;
};

#define RPC_GUARD(REQUEST, RESPONSE, DONE)                                                                             \
    auto guard = ServiceRpcGuard(DONE, RESPONSE);                                                                      \
    CatalogAccessLog catalogAccessLog(REQUEST, RESPONSE, __func__);                                                    \
    CATALOG_REQUIRES(RESPONSE, _serviceReady, Status::SERVICE_NOT_READY)

#define RPC_GUARD2(CATALOG_NAME, REQUEST, RESPONSE, DONE)                                                              \
    RPC_GUARD(REQUEST, RESPONSE, DONE);                                                                                \
    const auto &catalogName = (CATALOG_NAME);                                                                          \
    CATALOG_REQUIRES(RESPONSE, !catalogName.empty(), Status::INVALID_ARGUMENTS, "catalog_name is not specified");      \
    auto catalogController = _controllerManager->get(catalogName);                                                     \
    CATALOG_REQUIRES(RESPONSE, catalogController, Status::NOT_FOUND, "catalog:[", catalogName, "] not found")

CatalogServiceImpl::CatalogServiceImpl(const std::string &storeUri) : _serviceReady(false), _storeUri(storeUri) {}

CatalogServiceImpl::~CatalogServiceImpl() {}

bool CatalogServiceImpl::start() {
    _store = StoreFactory::getInstance()->create(_storeUri);
    if (!_store) {
        AUTIL_LOG(ERROR, "failed to create store with uri:[%s]", _storeUri.c_str());
        return false;
    }
    _controllerManager = std::make_shared<CatalogControllerManager>(_store);
    if (!_controllerManager->recover()) {
        AUTIL_LOG(ERROR, "failed to recover");
        return false;
    }
    AUTIL_LOG(INFO, "catalog service is ready");
    _serviceReady = true;
    return true;
}

void CatalogServiceImpl::stop() { _serviceReady = false; }

void CatalogServiceImpl::listCatalog(::google::protobuf::RpcController *controller,
                                     const proto::ListCatalogRequest *request,
                                     proto::ListCatalogResponse *response,
                                     ::google::protobuf::Closure *done) {
    RPC_GUARD(request, response, done);
    const auto &catalogNames = _controllerManager->list();
    *(response->mutable_catalog_names()) = {catalogNames.begin(), catalogNames.end()};
    if (!response->has_status()) {
        response->mutable_status()->set_code(Status::OK);
    }
}

void CatalogServiceImpl::getCatalog(::google::protobuf::RpcController *controller,
                                    const proto::GetCatalogRequest *request,
                                    proto::GetCatalogResponse *response,
                                    ::google::protobuf::Closure *done) {
    RPC_GUARD(request, response, done);
    const auto &catalogName = request->catalog_name();
    CATALOG_REQUIRES(response, !catalogName.empty(), Status::INVALID_ARGUMENTS, "catalog_name is not specified");
    auto catalogController = _controllerManager->get(catalogName);
    if (catalogController == nullptr) {
        CATALOG_REQUIRES(
            response, request->ignore_not_found_error(), Status::NOT_FOUND, "catalog:[", catalogName, "] not found");
    } else {
        catalogController->getCatalog(request, response);
    }
}

void CatalogServiceImpl::createCatalog(::google::protobuf::RpcController *controller,
                                       const proto::CreateCatalogRequest *request,
                                       proto::CommonResponse *response,
                                       ::google::protobuf::Closure *done) {
    RPC_GUARD(request, response, done);
    const auto &catalogName = request->catalog().catalog_name();
    CATALOG_REQUIRES(response, !catalogName.empty(), Status::INVALID_ARGUMENTS, "catalog_name is not specified");
    auto status = _controllerManager->tryAdd(catalogName, [&]() -> std::unique_ptr<CatalogController> {
        auto catalogController = std::make_unique<CatalogController>(catalogName, _store);
        if (!catalogController->init()) {
            return nullptr;
        }
        catalogController->createCatalog(request, response);
        if (isOk(response->status())) {
            return catalogController;
        } else {
            return nullptr;
        }
    });
    CATALOG_REQUIRES_OK(response, status);
}

void CatalogServiceImpl::dropCatalog(::google::protobuf::RpcController *controller,
                                     const proto::DropCatalogRequest *request,
                                     proto::CommonResponse *response,
                                     ::google::protobuf::Closure *done) {
    RPC_GUARD(request, response, done);
    const auto &catalogName = request->catalog_name();
    CATALOG_REQUIRES(response, !catalogName.empty(), Status::INVALID_ARGUMENTS, "catalog_name is not specified");
    auto status = _controllerManager->tryRemove(catalogName, [&](CatalogControllerPtr catalogController) {
        catalogController->dropCatalog(request, response);
        return isOk(response->status());
    });
    CATALOG_REQUIRES_OK(response, status);
}

void CatalogServiceImpl::updateCatalog(::google::protobuf::RpcController *controller,
                                       const proto::UpdateCatalogRequest *request,
                                       proto::CommonResponse *response,
                                       ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog().catalog_name(), request, response, done);
    catalogController->updateCatalog(request, response);
}

void CatalogServiceImpl::updateCatalogStatus(::google::protobuf::RpcController *controller,
                                             const proto::UpdateCatalogStatusRequest *request,
                                             proto::CommonResponse *response,
                                             ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->updateCatalogStatus(request, response);
}

void CatalogServiceImpl::listDatabase(::google::protobuf::RpcController *controller,
                                      const proto::ListDatabaseRequest *request,
                                      proto::ListDatabaseResponse *response,
                                      ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->listDatabase(request, response);
}
void CatalogServiceImpl::getDatabase(::google::protobuf::RpcController *controller,
                                     const proto::GetDatabaseRequest *request,
                                     proto::GetDatabaseResponse *response,
                                     ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->getDatabase(request, response);
}

void CatalogServiceImpl::createDatabase(::google::protobuf::RpcController *controller,
                                        const proto::CreateDatabaseRequest *request,
                                        proto::CommonResponse *response,
                                        ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->database().catalog_name(), request, response, done);
    catalogController->createDatabase(request, response);
}

void CatalogServiceImpl::dropDatabase(::google::protobuf::RpcController *controller,
                                      const proto::DropDatabaseRequest *request,
                                      proto::CommonResponse *response,
                                      ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->dropDatabase(request, response);
}

void CatalogServiceImpl::updateDatabase(::google::protobuf::RpcController *controller,
                                        const proto::UpdateDatabaseRequest *request,
                                        proto::CommonResponse *response,
                                        ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->database().catalog_name(), request, response, done);
    catalogController->updateDatabase(request, response);
}

void CatalogServiceImpl::listTable(::google::protobuf::RpcController *controller,
                                   const proto::ListTableRequest *request,
                                   proto::ListTableResponse *response,
                                   ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->listTable(request, response);
}

void CatalogServiceImpl::getTable(::google::protobuf::RpcController *controller,
                                  const proto::GetTableRequest *request,
                                  proto::GetTableResponse *response,
                                  ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->getTable(request, response);
}

void CatalogServiceImpl::createTable(::google::protobuf::RpcController *controller,
                                     const proto::CreateTableRequest *request,
                                     proto::CommonResponse *response,
                                     ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->table().catalog_name(), request, response, done);
    catalogController->createTable(request, response);
}

void CatalogServiceImpl::dropTable(::google::protobuf::RpcController *controller,
                                   const proto::DropTableRequest *request,
                                   proto::CommonResponse *response,
                                   ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->dropTable(request, response);
}

void CatalogServiceImpl::updateTable(::google::protobuf::RpcController *controller,
                                     const proto::UpdateTableRequest *request,
                                     proto::CommonResponse *response,
                                     ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->table().catalog_name(), request, response, done);
    catalogController->updateTable(request, response);
}

void CatalogServiceImpl::listTableRelatedTableGroup(::google::protobuf::RpcController *controller,
                                                    const proto::ListTableRelatedTableGroupRequest *request,
                                                    proto::ListTableRelatedTableGroupResponse *response,
                                                    ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->listTableRelatedTableGroup(request, response);
}

void CatalogServiceImpl::getTableStructure(::google::protobuf::RpcController *controller,
                                           const proto::GetTableStructureRequest *request,
                                           proto::GetTableStructureResponse *response,
                                           ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->getTableStructure(request, response);
}

void CatalogServiceImpl::updateTableStructure(::google::protobuf::RpcController *controller,
                                              const proto::UpdateTableStructureRequest *request,
                                              proto::CommonResponse *response,
                                              ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->table_structure().catalog_name(), request, response, done);
    catalogController->updateTableStructure(request, response);
}

void CatalogServiceImpl::addColumn(::google::protobuf::RpcController *controller,
                                   const proto::AddColumnRequest *request,
                                   proto::CommonResponse *response,
                                   ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->addColumn(request, response);
}

void CatalogServiceImpl::updateColumn(::google::protobuf::RpcController *controller,
                                      const proto::UpdateColumnRequest *request,
                                      proto::CommonResponse *response,
                                      ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->updateColumn(request, response);
}

void CatalogServiceImpl::dropColumn(::google::protobuf::RpcController *controller,
                                    const proto::DropColumnRequest *request,
                                    proto::CommonResponse *response,
                                    ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->dropColumn(request, response);
}

void CatalogServiceImpl::createIndex(::google::protobuf::RpcController *controller,
                                     const proto::CreateIndexRequest *request,
                                     proto::CommonResponse *response,
                                     ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->createIndex(request, response);
}

void CatalogServiceImpl::updateIndex(::google::protobuf::RpcController *controller,
                                     const proto::UpdateIndexRequest *request,
                                     proto::CommonResponse *response,
                                     ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->updateIndex(request, response);
}

void CatalogServiceImpl::dropIndex(::google::protobuf::RpcController *controller,
                                   const proto::DropIndexRequest *request,
                                   proto::CommonResponse *response,
                                   ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->dropIndex(request, response);
}

void CatalogServiceImpl::listPartition(::google::protobuf::RpcController *controller,
                                       const proto::ListPartitionRequest *request,
                                       proto::ListPartitionResponse *response,
                                       ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->listPartition(request, response);
}

void CatalogServiceImpl::getPartition(::google::protobuf::RpcController *controller,
                                      const proto::GetPartitionRequest *request,
                                      proto::GetPartitionResponse *response,
                                      ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->getPartition(request, response);
}

void CatalogServiceImpl::createPartition(::google::protobuf::RpcController *controller,
                                         const proto::CreatePartitionRequest *request,
                                         proto::CommonResponse *response,
                                         ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->partition().catalog_name(), request, response, done);
    catalogController->createPartition(request, response);
}

void CatalogServiceImpl::dropPartition(::google::protobuf::RpcController *controller,
                                       const proto::DropPartitionRequest *request,
                                       proto::CommonResponse *response,
                                       ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->dropPartition(request, response);
}

void CatalogServiceImpl::updatePartition(::google::protobuf::RpcController *controller,
                                         const proto::UpdatePartitionRequest *request,
                                         proto::CommonResponse *response,
                                         ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->partition().catalog_name(), request, response, done);
    catalogController->updatePartition(request, response);
}

void CatalogServiceImpl::updatePartitionTableStructure(::google::protobuf::RpcController *controller,
                                                       const proto::UpdatePartitionTableStructureRequest *request,
                                                       proto::CommonResponse *response,
                                                       ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->updatePartitionTableStructure(request, response);
}

void CatalogServiceImpl::listTableGroup(::google::protobuf::RpcController *controller,
                                        const proto::ListTableGroupRequest *request,
                                        proto::ListTableGroupResponse *response,
                                        ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->listTableGroup(request, response);
}

void CatalogServiceImpl::getTableGroup(::google::protobuf::RpcController *controller,
                                       const proto::GetTableGroupRequest *request,
                                       proto::GetTableGroupResponse *response,
                                       ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->getTableGroup(request, response);
}

void CatalogServiceImpl::createTableGroup(::google::protobuf::RpcController *controller,
                                          const proto::CreateTableGroupRequest *request,
                                          proto::CommonResponse *response,
                                          ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->table_group().catalog_name(), request, response, done);
    catalogController->createTableGroup(request, response);
}

void CatalogServiceImpl::dropTableGroup(::google::protobuf::RpcController *controller,
                                        const proto::DropTableGroupRequest *request,
                                        proto::CommonResponse *response,
                                        ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->dropTableGroup(request, response);
}

void CatalogServiceImpl::updateTableGroup(::google::protobuf::RpcController *controller,
                                          const proto::UpdateTableGroupRequest *request,
                                          proto::CommonResponse *response,
                                          ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->table_group().catalog_name(), request, response, done);
    catalogController->updateTableGroup(request, response);
}

void CatalogServiceImpl::listLoadStrategy(::google::protobuf::RpcController *controller,
                                          const proto::ListLoadStrategyRequest *request,
                                          proto::ListLoadStrategyResponse *response,
                                          ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->listLoadStrategy(request, response);
}

void CatalogServiceImpl::getLoadStrategy(::google::protobuf::RpcController *controller,
                                         const proto::GetLoadStrategyRequest *request,
                                         proto::GetLoadStrategyResponse *response,
                                         ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->getLoadStrategy(request, response);
}

void CatalogServiceImpl::createLoadStrategy(::google::protobuf::RpcController *controller,
                                            const proto::CreateLoadStrategyRequest *request,
                                            proto::CommonResponse *response,
                                            ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->load_strategy().catalog_name(), request, response, done);
    catalogController->createLoadStrategy(request, response);
}

void CatalogServiceImpl::dropLoadStrategy(::google::protobuf::RpcController *controller,
                                          const proto::DropLoadStrategyRequest *request,
                                          proto::CommonResponse *response,
                                          ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->dropLoadStrategy(request, response);
}

void CatalogServiceImpl::updateLoadStrategy(::google::protobuf::RpcController *controller,
                                            const proto::UpdateLoadStrategyRequest *request,
                                            proto::CommonResponse *response,
                                            ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->load_strategy().catalog_name(), request, response, done);
    catalogController->updateLoadStrategy(request, response);
}

void CatalogServiceImpl::listFunction(::google::protobuf::RpcController *controller,
                                      const proto::ListFunctionRequest *request,
                                      proto::ListFunctionResponse *response,
                                      ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->listFunction(request, response);
}

void CatalogServiceImpl::getFunction(::google::protobuf::RpcController *controller,
                                     const proto::GetFunctionRequest *request,
                                     proto::GetFunctionResponse *response,
                                     ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->getFunction(request, response);
}

void CatalogServiceImpl::createFunction(::google::protobuf::RpcController *controller,
                                        const proto::CreateFunctionRequest *request,
                                        proto::CommonResponse *response,
                                        ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->function().catalog_name(), request, response, done);
    catalogController->createFunction(request, response);
}

void CatalogServiceImpl::dropFunction(::google::protobuf::RpcController *controller,
                                      const proto::DropFunctionRequest *request,
                                      proto::CommonResponse *response,
                                      ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->dropFunction(request, response);
}

void CatalogServiceImpl::updateFunction(::google::protobuf::RpcController *controller,
                                        const proto::UpdateFunctionRequest *request,
                                        proto::CommonResponse *response,
                                        ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->function().catalog_name(), request, response, done);
    catalogController->updateFunction(request, response);
}

void CatalogServiceImpl::listBuild(::google::protobuf::RpcController *controller,
                                   const proto::ListBuildRequest *request,
                                   proto::ListBuildResponse *response,
                                   ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->catalog_name(), request, response, done);
    catalogController->listBuild(request, response);
}

void CatalogServiceImpl::getBuild(::google::protobuf::RpcController *controller,
                                  const proto::GetBuildRequest *request,
                                  proto::GetBuildResponse *response,
                                  ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->build_id().catalog_name(), request, response, done);
    catalogController->getBuild(request, response);
}

void CatalogServiceImpl::createBuild(::google::protobuf::RpcController *controller,
                                     const proto::CreateBuildRequest *request,
                                     proto::CreateBuildResponse *response,
                                     ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->partition().catalog_name(), request, response, done);
    catalogController->createBuild(request, response);
}

void CatalogServiceImpl::dropBuild(::google::protobuf::RpcController *controller,
                                   const proto::DropBuildRequest *request,
                                   proto::BuildCommonResponse *response,
                                   ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->build_id().catalog_name(), request, response, done);
    catalogController->dropBuild(request, response);
}

void CatalogServiceImpl::updateBuild(::google::protobuf::RpcController *controller,
                                     const proto::UpdateBuildRequest *request,
                                     proto::BuildCommonResponse *response,
                                     ::google::protobuf::Closure *done) {
    RPC_GUARD2(request->build().build_id().catalog_name(), request, response, done);
    catalogController->updateBuild(request, response);
}

std::shared_ptr<CatalogControllerManager> CatalogServiceImpl::getManager() { return _controllerManager; }

#undef RPC_GUARD2
#undef RPC_GUARD

} // namespace catalog
