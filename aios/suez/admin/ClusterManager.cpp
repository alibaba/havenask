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
#include "suez/admin/ClusterManager.h"

#include "autil/Log.h"
#include "google/protobuf/util/message_differencer.h"

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, ClusterManager);

ClusterManager::ClusterManager(std::unique_ptr<ClusterServiceState> state) : _state(std::move(state)) {}

ClusterManager::~ClusterManager() = default;

bool ClusterManager::recover() {
    auto s = _state->recover();
    if (!s.is_ok()) {
        return false;
    }

    auto stateEntity = s.get();
    for (const auto &clusterDeployment : stateEntity.clusterdeployments()) {
        _clusterDeploymentMap.emplace(clusterDeployment.deploymentname(), clusterDeployment);
    }
    for (const auto &dbDeployment : stateEntity.databasedeployments()) {
        _dbDeploymentMap.emplace(std::make_pair(dbDeployment.catalogname(), dbDeployment.databasename()), dbDeployment);
    }

    return true;
}

void ClusterManager::createClusterDeployment(const CreateClusterDeploymentRequest *request, CommonResponse *response) {
    autil::ScopedWriteLock lock(_lock);

    const auto &deployment = request->deployment();
    if (_clusterDeploymentMap.count(deployment.deploymentname()) > 0) {
        response->set_errorcode(CommonResponse::ERROR_EXIST);
        response->set_errormsg("deployment " + deployment.deploymentname() + " exist");
        return;
    }

    std::map<std::string, ClusterDeployment> newMap = _clusterDeploymentMap;
    newMap.emplace(deployment.deploymentname(), deployment);
    if (!_state->persist(newMap, _dbDeploymentMap)) {
        response->set_errorcode(CommonResponse::ERROR_INTERNAL);
        response->set_errormsg("persist failed when create cluster deployment: " + deployment.deploymentname());
        return;
    }
    _clusterDeploymentMap = newMap;
}

void ClusterManager::deleteCluster(const DeleteClusterRequest *request, CommonResponse *response) {
    autil::ScopedWriteLock lock(_lock);

    const auto &deploymentName = request->deploymentname();
    const auto &clusterName = request->clustername();
    if (_clusterDeploymentMap.count(deploymentName) == 0) {
        response->set_errorcode(CommonResponse::ERROR_NOT_FOUND);
        response->set_errormsg("deployment " + deploymentName + " not found");
        return;
    }
    std::map<std::string, ClusterDeployment> newMap = _clusterDeploymentMap;
    const auto &deployment = newMap[request->deploymentname()];
    ClusterDeployment newDeployment;
    newDeployment.set_deploymentname(request->deploymentname());
    newDeployment.mutable_config()->CopyFrom(deployment.config());

    for (const auto &cluster : deployment.clusters()) {
        if (cluster.clustername() != clusterName) {
            newDeployment.mutable_clusters()->Add()->CopyFrom(cluster);
        }
    }

    newMap[request->deploymentname()] = newDeployment;

    if (!_state->persist(newMap, _dbDeploymentMap)) {
        response->set_errorcode(CommonResponse::ERROR_INTERNAL);
        response->set_errormsg("persist failed when delete cluster deployment: " + deployment.deploymentname());
        return;
    }

    _clusterDeploymentMap = newMap;
}

void ClusterManager::createCluster(const CreateClusterRequest *request, CommonResponse *response) {
    autil::ScopedWriteLock lock(_lock);

    const auto &deploymentName = request->deploymentname();
    const auto &clusterName = request->cluster().clustername();
    if (_clusterDeploymentMap.count(deploymentName) == 0) {
        response->set_errorcode(CommonResponse::ERROR_NOT_FOUND);
        response->set_errormsg("deployment " + deploymentName + " not found");
        return;
    }
    std::map<std::string, ClusterDeployment> newMap = _clusterDeploymentMap;
    auto &deployment = newMap[request->deploymentname()];
    for (const auto &cluster : deployment.clusters()) {
        if (cluster.clustername() == clusterName) {
            response->set_errorcode(CommonResponse::ERROR_EXIST);
            response->set_errormsg("cluster " + clusterName + " exist");
            return;
        }
    }
    deployment.mutable_clusters()->Add()->CopyFrom(request->cluster());

    if (!_state->persist(newMap, _dbDeploymentMap)) {
        response->set_errorcode(CommonResponse::ERROR_INTERNAL);
        response->set_errormsg("persist failed when create cluster deployment: " + deployment.deploymentname());
        return;
    }
    _clusterDeploymentMap = newMap;
}

void ClusterManager::updateCluster(const UpdateClusterRequest *request, CommonResponse *response) {
    autil::ScopedWriteLock lock(_lock);

    const auto &deploymentName = request->deploymentname();
    const auto &clusterName = request->cluster().clustername();
    if (_clusterDeploymentMap.count(deploymentName) == 0) {
        response->set_errorcode(CommonResponse::ERROR_NOT_FOUND);
        response->set_errormsg("deployment " + deploymentName + " not found");
        return;
    }

    std::map<std::string, ClusterDeployment> newMap = _clusterDeploymentMap;
    auto &deployment = newMap[request->deploymentname()];
    for (size_t i = 0; i < deployment.clusters_size(); i++) {
        if (deployment.clusters(i).clustername() == clusterName) {
            deployment.mutable_clusters(i)->CopyFrom(request->cluster());
            if (!_state->persist(newMap, _dbDeploymentMap)) {
                response->set_errorcode(CommonResponse::ERROR_INTERNAL);
                response->set_errormsg("persist failed when update cluster deployment: " + deployment.deploymentname());
                return;
            }
            _clusterDeploymentMap = newMap;
            return;
        }
    }

    response->set_errorcode(CommonResponse::ERROR_NOT_FOUND);
    response->set_errormsg("cluster " + clusterName + " not found");
}

void ClusterManager::deployDatabase(const DeployDatabaseRequest *request, CommonResponse *response) {
    autil::ScopedWriteLock lock(_lock);

    std::map<std::pair<std::string, std::string>, DatabaseDeployment> newMap = _dbDeploymentMap;
    newMap.emplace(
        std::make_pair(request->databasedeployment().catalogname(), request->databasedeployment().databasename()),
        request->databasedeployment());
    if (!_state->persist(_clusterDeploymentMap, newMap)) {
        response->set_errorcode(CommonResponse::ERROR_INTERNAL);
        response->set_errormsg("deploy database persist failed");
        return;
    }
    _dbDeploymentMap = newMap;
}

void ClusterManager::deleteDatabaseDeployment(const DeleteDatabaseDeploymentRequest *request,
                                              CommonResponse *response) {
    autil::ScopedWriteLock lock(_lock);

    auto key = std::make_pair(request->catalogname(), request->databasename());
    if (_dbDeploymentMap.count(key) == 0) {
        response->set_errorcode(CommonResponse::ERROR_NOT_FOUND);
        response->set_errormsg("not found database depoyment");
        return;
    }

    std::map<std::pair<std::string, std::string>, DatabaseDeployment> newMap = _dbDeploymentMap;
    newMap.erase(key);

    if (!_state->persist(_clusterDeploymentMap, newMap)) {
        response->set_errorcode(CommonResponse::ERROR_INTERNAL);
        response->set_errormsg("delete database deployment  persist failed");
        return;
    }
    _dbDeploymentMap = newMap;
}

void ClusterManager::getClusterDeployment(const GetClusterDeploymentRequest *request,
                                          GetClusterDeploymentResponse *response) {
    autil::ScopedReadLock lock(_lock);
    const auto &deploymentName = request->deploymentname();
    if (_clusterDeploymentMap.count(deploymentName) > 0) {
        response->mutable_clusterdeployment()->CopyFrom(_clusterDeploymentMap[deploymentName]);
    }
}

void ClusterManager::getDatabaseDeployment(const GetDatabaseDeploymentRequest *request,
                                           GetDatabaseDeploymentResponse *response) {
    autil::ScopedReadLock lock(_lock);
    const auto &catalogName = request->catalogname();
    const auto &databaseName = request->databasename();
    auto key = std::make_pair(catalogName, databaseName);
    if (_dbDeploymentMap.count(key) > 0) {
        response->mutable_databasedeployment()->CopyFrom(_dbDeploymentMap[key]);
    }
}

std::map<std::pair<std::string, std::string>, DatabaseDeployment> ClusterManager::getDbDeploymentMap() {
    autil::ScopedReadLock lock(_lock);

    return _dbDeploymentMap;
}

std::map<std::string, ClusterDeployment> ClusterManager::getClusterDeploymentMap() {
    autil::ScopedReadLock lock(_lock);

    return _clusterDeploymentMap;
}

} // namespace suez
