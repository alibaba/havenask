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

#include "aios/network/arpc/arpc/ANetRPCServer.h"
#include "autil/Lock.h"
#include "autil/LoopThread.h"
#include "carbon/RoleManagerWrapper.h"
#include "catalog/service/CatalogServiceImpl.h"
#include "suez/admin/CarbonAdapter.h"
#include "suez/admin/ClusterServiceImpl.h"
#include "suez/admin/SchedulerService.pb.h"
#include "suez/admin/TargetGenerator.h"

namespace suez {

class SchedulerServiceImpl : public SchedulerService {
public:
    SchedulerServiceImpl(bool localCarbon,
                         bool localCm2Mode,
                         const std::shared_ptr<carbon::RoleManagerWrapper> &roleManager);
    ~SchedulerServiceImpl();

private:
    SchedulerServiceImpl(const SchedulerServiceImpl &);
    SchedulerServiceImpl &operator=(const SchedulerServiceImpl &);

public:
    bool start(const std::shared_ptr<catalog::CatalogControllerManager> &catalogControllerManager,
               const std::shared_ptr<ClusterManager> &clusterManager);
    void stop();
    void getSystemInfo(::google::protobuf::RpcController *controller,
                       const GetSystemInfoRequest *request,
                       GetSystemInfoResponse *response,
                       google::protobuf::Closure *done) override;

private:
    void commitToCarbon();
    void fetchFromCarbon();
    void setStatus(const JsonNodeRef::JsonMap &status, std::string statusStr);
    std::pair<JsonNodeRef::JsonMap, std::string> getStatus() const;
    static void extractStatusInfo(const std::map<std::string, carbon::GroupStatus> groupStatusMap,
                                  JsonNodeRef::JsonMap *statusInfo);
    static void extractNodeStatusInfo(const carbon::proto::ReplicaNode &node, JsonNodeRef::JsonArray *roleStatusInfo);
    static std::string getCm2TopoStr(const std::string &serviceInfo);
    static bool parseCm2TopoStr(const std::string &topoStr,
                                std::string &bizName,
                                int32_t &partCnt,
                                int32_t &partId,
                                int64_t &version,
                                int32_t &grpcPort,
                                bool &supportHeartbeat);
    static void simplifyGroupsStatus(std::map<std::string, carbon::GroupStatus> &groupStatusMap);
    static void reRangeGroupStatus(std::map<std::string, carbon::GroupStatus> &groupStatusMap);

private:
    bool _serviceReady = false;
    mutable autil::ReadWriteLock _lock;

    autil::LoopThreadPtr _loopThread;
    autil::LoopThreadPtr _fetchLoopThread;

    std::unique_ptr<CarbonAdapter> _carbonAdapter;
    std::unique_ptr<TargetGenerator> _targetGenerator;
    bool _localCarbon;
    bool _localCm2Mode;
    std::shared_ptr<carbon::RoleManagerWrapper> _roleManager;

    JsonNodeRef::JsonMap _status;
    std::string _lastStatusStr;
    mutable autil::ReadWriteLock _statusLock;
    std::string _lastCommitStatusStr;
    std::string _lastCommitTargetStr;
};

} // namespace suez
