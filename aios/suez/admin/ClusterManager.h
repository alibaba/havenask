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

#include <map>

#include "autil/Lock.h"
#include "suez/admin/ClusterServiceState.h"

namespace suez {

class ClusterManager {
public:
    ClusterManager(std::unique_ptr<ClusterServiceState> store);
    ~ClusterManager();

public:
    bool recover();
    void createClusterDeployment(const CreateClusterDeploymentRequest *request, CommonResponse *response);
    void createCluster(const CreateClusterRequest *request, CommonResponse *response);
    void deleteCluster(const DeleteClusterRequest *request, CommonResponse *response);
    void updateCluster(const UpdateClusterRequest *request, CommonResponse *response);
    void deployDatabase(const DeployDatabaseRequest *request, CommonResponse *response);
    void deleteDatabaseDeployment(const DeleteDatabaseDeploymentRequest *request, CommonResponse *response);

    void getClusterDeployment(const GetClusterDeploymentRequest *request, GetClusterDeploymentResponse *response);
    void getDatabaseDeployment(const GetDatabaseDeploymentRequest *request, GetDatabaseDeploymentResponse *response);

    std::map<std::pair<std::string, std::string>, DatabaseDeployment> getDbDeploymentMap();
    std::map<std::string, ClusterDeployment> getClusterDeploymentMap();

private:
    autil::ReadWriteLock _lock;
    std::unique_ptr<ClusterServiceState> _state;
    std::map<std::string, ClusterDeployment> _clusterDeploymentMap;
    std::map<std::pair<std::string, std::string>, DatabaseDeployment> _dbDeploymentMap;
};

} // namespace suez
