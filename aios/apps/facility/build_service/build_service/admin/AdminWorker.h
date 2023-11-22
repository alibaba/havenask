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
#include "build_service/admin/ServiceKeeper.h"
#include "build_service/common_define.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/util/Log.h"
#include "worker_framework/LeaderedWorkerBase.h"

namespace build_service { namespace admin {

class AdminWorker : public worker_framework::LeaderedWorkerBase, public proto::AdminService
{
public:
    typedef MASTER_FRAMEWORK_NS(simple_master)::SimpleMasterScheduler SimpleMasterScheduler;
    typedef worker_framework::LeaderChecker LeaderChecker;

public:
    AdminWorker();
    ~AdminWorker();

private:
    AdminWorker(const AdminWorker&);
    AdminWorker& operator=(const AdminWorker&);

public:
    bool doInit() override;
    bool doStart() override;
    void doInitOptions() override;
    void doStop() override;

public:
    void startBuild(::google::protobuf::RpcController* controller, const proto::StartBuildRequest* request,
                    proto::StartBuildResponse* response, ::google::protobuf::Closure* done) override;
    void startTask(::google::protobuf::RpcController* controller, const proto::StartTaskRequest* request,
                   proto::InformResponse* response, ::google::protobuf::Closure* done) override;
    void stopTask(::google::protobuf::RpcController* controller, const proto::StopTaskRequest* request,
                  proto::InformResponse* response, ::google::protobuf::Closure* done) override;
    void stopBuild(::google::protobuf::RpcController* controller, const proto::StopBuildRequest* request,
                   proto::InformResponse* response, ::google::protobuf::Closure* done) override;
    void updateConfig(::google::protobuf::RpcController* controller, const proto::UpdateConfigRequest* request,
                      proto::InformResponse* response, ::google::protobuf::Closure* done) override;
    void stepBuild(::google::protobuf::RpcController* controller, const proto::StepBuildRequest* request,
                   proto::InformResponse* response, ::google::protobuf::Closure* done) override;
    void suspendBuild(::google::protobuf::RpcController* controller, const proto::SuspendBuildRequest* request,
                      proto::InformResponse* response, ::google::protobuf::Closure* done) override;
    void resumeBuild(::google::protobuf::RpcController* controller, const proto::ResumeBuildRequest* request,
                     proto::InformResponse* response, ::google::protobuf::Closure* done) override;
    void rollBack(::google::protobuf::RpcController* controller, const proto::RollBackRequest* request,
                  proto::InformResponse* response, ::google::protobuf::Closure* done) override;
    void rollBackCheckpoint(::google::protobuf::RpcController* controller,
                            const proto::RollBackCheckpointRequest* request, proto::InformResponse* response,
                            ::google::protobuf::Closure* done) override;
    void createSnapshot(::google::protobuf::RpcController* controller, const proto::CreateSnapshotRequest* request,
                        proto::InformResponse* response, ::google::protobuf::Closure* done) override;
    void removeSnapshot(::google::protobuf::RpcController* controller, const proto::RemoveSnapshotRequest* request,
                        proto::InformResponse* response, ::google::protobuf::Closure* done) override;
    void createSavepoint(::google::protobuf::RpcController* controller, const proto::CreateSavepointRequest* request,
                         proto::InformResponse* response, ::google::protobuf::Closure* done) override;
    void removeSavepoint(::google::protobuf::RpcController* controller, const proto::RemoveSavepointRequest* request,
                         proto::InformResponse* response, ::google::protobuf::Closure* done) override;
    void listCheckpoint(::google::protobuf::RpcController* controller, const proto::ListCheckpointRequest* request,
                        proto::InformResponse* response, ::google::protobuf::Closure* done) override;
    void commitVersion(::google::protobuf::RpcController* controller, const proto::CommitVersionRequest* request,
                       proto::InformResponse* response, ::google::protobuf::Closure* done) override;
    void getCommittedVersions(::google::protobuf::RpcController* controller,
                              const proto::GetCommittedVersionsRequest* request, proto::InformResponse* response,
                              ::google::protobuf::Closure* done) override;
    void getCheckpoint(::google::protobuf::RpcController* controller, const proto::GetCheckpointRequest* request,
                       proto::InformResponse* response, ::google::protobuf::Closure* done) override;
    void createVersion(::google::protobuf::RpcController* controller, const proto::CreateVersionRequest* request,
                       proto::InformResponse* response, ::google::protobuf::Closure* done) override;
    void switchBuild(::google::protobuf::RpcController* controller, const proto::SwitchBuildRequest* request,
                     proto::InformResponse* response, ::google::protobuf::Closure* done) override;
    void getTaskInfo(::google::protobuf::RpcController* controller, const proto::TaskInfoRequest* request,
                     proto::TaskInfoResponse* response, ::google::protobuf::Closure* done) override;
    void getServiceInfo(::google::protobuf::RpcController* controller, const proto::ServiceInfoRequest* request,
                        proto::ServiceInfoResponse* response, ::google::protobuf::Closure* done) override;
    void setLoggerLevel(::google::protobuf::RpcController* controller, const proto::SetLoggerLevelRequest* request,
                        proto::InformResponse* response, ::google::protobuf::Closure* done) override;
    void cleanVersions(::google::protobuf::RpcController* controller, const proto::CleanVersionsRequest* request,
                       proto::InformResponse* response, ::google::protobuf::Closure* done) override;
    void callGraph(::google::protobuf::RpcController* controller, const proto::CallGraphRequest* request,
                   proto::InformResponse* response, ::google::protobuf::Closure* done) override;

    void printGraph(::google::protobuf::RpcController* controller, const proto::PrintGraphRequest* request,
                    proto::InformResponse* response, ::google::protobuf::Closure* done) override;

    void updateGsTemplate(::google::protobuf::RpcController* controller, const proto::UpdateGsTemplateRequest* request,
                          proto::InformResponse* response, ::google::protobuf::Closure* done) override;

    void startGeneralTask(::google::protobuf::RpcController* controller, const proto::StartGeneralTaskRequest* request,
                          proto::InformResponse* response, ::google::protobuf::Closure* done) override;

    void migrateTargetRoles(::google::protobuf::RpcController* controller, const proto::MigrateRoleRequest* request,
                            proto::InformResponse* response, ::google::protobuf::Closure* done) override;

    void updateGlobalAgentConfig(::google::protobuf::RpcController* controller,
                                 const proto::UpdateGlobalAgentConfigRequest* request, proto::InformResponse* response,
                                 ::google::protobuf::Closure* done) override;

    void getWorkerRoleInfo(::google::protobuf::RpcController* controller, const proto::WorkerRoleInfoRequest* request,
                           proto::WorkerRoleInfoResponse* response, ::google::protobuf::Closure* done) override;

    void getWorkerNodeStatus(::google::protobuf::RpcController* controller,
                             const proto::WorkerNodeStatusRequest* request, proto::WorkerNodeStatusResponse* response,
                             ::google::protobuf::Closure* done) override;

    void getGeneralInfo(::google::protobuf::RpcController* controller, const proto::InformRequest* request,
                        proto::InformResponse* response, ::google::protobuf::Closure* done) override;

    void getBulkloadInfo(::google::protobuf::RpcController* controller, const proto::BulkloadInfoRequest* request,
                         proto::InformResponse* response, ::google::protobuf::Closure* done) override;

    void bulkload(::google::protobuf::RpcController* controller, const proto::BulkloadRequest* request,
                  proto::InformResponse* response, ::google::protobuf::Closure* done) override;

private:
    void initBeeper();
    void initEnvArgs();
    bool startMonitor();
    static bool initLocalLuaPath(const std::string& zkRoot);

protected:
    void doIdle() override;
    void becomeLeader() override {}
    void noLongerLeader() override;

public:
    // public for test
    void setServiceKeeper(ServiceKeeper* keeper)
    {
        delete _keeper;
        _keeper = keeper;
    }
    // virtual for test
    virtual SimpleMasterScheduler* createSimpleMasterScheduler() const;

private:
    bool setLeaderInfo();
    static const size_t RPC_SERVICE_THREAD_NUM;
    static const size_t RPC_SERVICE_QUEUE_SIZE;
    static const std::string RPC_SERVICE_THEAD_POOL_NAME;

private:
    ServiceKeeper* _keeper = nullptr;
    mutable bool _isStarted;
    kmonitor_adapter::Monitor* _monitor;

private:
    static const uint32_t DEFAULT_ZK_TIMEOUT = 10; // in seconds, 10s
    static const uint32_t DEFAULT_PROHIBIT_TIME = 2 * 60 * 60;
    static const uint32_t DEFAULT_PROHIBIT_NUM = 5;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::admin
