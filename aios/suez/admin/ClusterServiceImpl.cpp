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
#include "suez/admin/ClusterServiceImpl.h"

#include "autil/ClosureGuard.h"
#include "autil/Log.h"

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, ClusterServiceImp);

#define RPC_GUARD()                                                                                                    \
    autil::ClosureGuard guard(done);                                                                                   \
    do {                                                                                                               \
        if (!_serviceReady) {                                                                                          \
            response->set_errorcode(CommonResponse::ERROR_SERVICE_NOT_READY);                                          \
            response->set_errormsg("service not ready, drop rpc request");                                             \
        }                                                                                                              \
    } while (0)

ClusterServiceImpl::ClusterServiceImpl(const std::string &storeUri) : _serviceReady(false), _storeUri(storeUri) {}

ClusterServiceImpl::~ClusterServiceImpl() = default;

bool ClusterServiceImpl::start() {
    auto store = ClusterServiceState::create(_storeUri);
    if (store == nullptr) {
        AUTIL_LOG(ERROR, "failed to create cluster store with uri: [%s]", _storeUri.c_str());
        return false;
    }

    _clusterManager = std::make_shared<ClusterManager>(std::move(store));
    if (!_clusterManager->recover()) {
        AUTIL_LOG(ERROR, "cluster manager recover failed");
        return false;
    }

    _serviceReady = true;
    return true;
}

void ClusterServiceImpl::stop() {
    autil::ScopedWriteLock lock(_lock);
    _serviceReady = false;
}

void ClusterServiceImpl::createClusterDeployment(::google::protobuf::RpcController *controller,
                                                 const CreateClusterDeploymentRequest *request,
                                                 CommonResponse *response,
                                                 google::protobuf::Closure *done) {
    autil::ScopedWriteLock lock(_lock);
    RPC_GUARD();

    _clusterManager->createClusterDeployment(request, response);
}

void ClusterServiceImpl::createCluster(::google::protobuf::RpcController *controller,
                                       const CreateClusterRequest *request,
                                       CommonResponse *response,
                                       google::protobuf::Closure *done) {
    autil::ScopedWriteLock lock(_lock);
    RPC_GUARD();

    _clusterManager->createCluster(request, response);
}

void ClusterServiceImpl::deleteCluster(::google::protobuf::RpcController *controller,
                                       const DeleteClusterRequest *request,
                                       CommonResponse *response,
                                       google::protobuf::Closure *done) {
    autil::ScopedWriteLock lock(_lock);
    RPC_GUARD();

    _clusterManager->deleteCluster(request, response);
}

void ClusterServiceImpl::updateCluster(::google::protobuf::RpcController *controller,
                                       const UpdateClusterRequest *request,
                                       CommonResponse *response,
                                       google::protobuf::Closure *done) {
    autil::ScopedWriteLock lock(_lock);
    RPC_GUARD();

    _clusterManager->updateCluster(request, response);
}

void ClusterServiceImpl::deployDatabase(::google::protobuf::RpcController *controller,
                                        const DeployDatabaseRequest *request,
                                        CommonResponse *response,
                                        google::protobuf::Closure *done) {
    autil::ScopedWriteLock lock(_lock);
    RPC_GUARD();

    _clusterManager->deployDatabase(request, response);
}

void ClusterServiceImpl::deleteDatabaseDeployment(::google::protobuf::RpcController *controller,
                                                  const DeleteDatabaseDeploymentRequest *request,
                                                  CommonResponse *response,
                                                  google::protobuf::Closure *done) {
    autil::ScopedWriteLock lock(_lock);
    RPC_GUARD();

    _clusterManager->deleteDatabaseDeployment(request, response);
}

void ClusterServiceImpl::getClusterDeployment(::google::protobuf::RpcController *controller,
                                              const GetClusterDeploymentRequest *request,
                                              GetClusterDeploymentResponse *response,
                                              google::protobuf::Closure *done) {
    autil::ScopedReadLock lock(_lock);
    autil::ClosureGuard guard(done);

    if (!_serviceReady) {
        return;
    }

    _clusterManager->getClusterDeployment(request, response);
}

void ClusterServiceImpl::getDatabaseDeployment(::google::protobuf::RpcController *controller,
                                               const GetDatabaseDeploymentRequest *request,
                                               GetDatabaseDeploymentResponse *response,
                                               google::protobuf::Closure *done) {
    autil::ScopedReadLock lock(_lock);
    autil::ClosureGuard guard(done);

    if (!_serviceReady) {
        return;
    }

    _clusterManager->getDatabaseDeployment(request, response);
}

std::shared_ptr<ClusterManager> ClusterServiceImpl::getManager() { return _clusterManager; }

} // namespace suez
