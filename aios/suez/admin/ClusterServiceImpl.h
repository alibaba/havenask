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

#include "autil/Lock.h"
#include "suez/admin/ClusterManager.h"

namespace suez {

class ClusterServiceImpl : public ClusterService {
public:
    ClusterServiceImpl(const std::string &storeUri);
    ~ClusterServiceImpl();

private:
    ClusterServiceImpl(const ClusterServiceImpl &);
    ClusterServiceImpl &operator=(const ClusterServiceImpl &);

public:
    bool start();
    void stop();
    void createClusterDeployment(::google::protobuf::RpcController *controller,
                                 const CreateClusterDeploymentRequest *request,
                                 CommonResponse *response,
                                 google::protobuf::Closure *done) override;

    void createCluster(::google::protobuf::RpcController *controller,
                       const CreateClusterRequest *request,
                       CommonResponse *response,
                       google::protobuf::Closure *done) override;

    void deleteCluster(::google::protobuf::RpcController *controller,
                       const DeleteClusterRequest *request,
                       CommonResponse *response,
                       google::protobuf::Closure *done) override;

    void updateCluster(::google::protobuf::RpcController *controller,
                       const UpdateClusterRequest *request,
                       CommonResponse *response,
                       google::protobuf::Closure *done) override;

    void deployDatabase(::google::protobuf::RpcController *controller,
                        const DeployDatabaseRequest *request,
                        CommonResponse *response,
                        google::protobuf::Closure *done) override;

    void deleteDatabaseDeployment(::google::protobuf::RpcController *controller,
                                  const DeleteDatabaseDeploymentRequest *request,
                                  CommonResponse *response,
                                  google::protobuf::Closure *done) override;

    void getClusterDeployment(::google::protobuf::RpcController *controller,
                              const GetClusterDeploymentRequest *request,
                              GetClusterDeploymentResponse *response,
                              google::protobuf::Closure *done) override;

    void getDatabaseDeployment(::google::protobuf::RpcController *controller,
                               const GetDatabaseDeploymentRequest *request,
                               GetDatabaseDeploymentResponse *response,
                               google::protobuf::Closure *done) override;

    std::shared_ptr<ClusterManager> getManager();

private:
    mutable autil::ReadWriteLock _lock;
    std::shared_ptr<ClusterManager> _clusterManager;
    bool _serviceReady;
    std::string _storeUri;
};

} // namespace suez
