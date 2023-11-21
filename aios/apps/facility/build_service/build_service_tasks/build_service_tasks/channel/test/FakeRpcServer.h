#pragma once

#include <google/protobuf/service.h>
#include <google/protobuf/stubs/callback.h>
#include <memory>
#include <thread>

#include "build_service/proto/Admin.pb.h"
#include "build_service/util/Log.h"
#include "build_service_tasks/channel/Master.pb.h"
#include "worker_framework/WorkerBase.h"

namespace build_service { namespace task_base {

class FakeBsAdmin : public worker_framework::WorkerBase, public proto::AdminService
{
public:
    FakeBsAdmin() = default;
    ~FakeBsAdmin();

public:
    bool isRunning() const { return _isRunning; }
    bool startServer();
    bool workLoop();

public:
    bool doInit() override;
    bool doStart() override
    {
        _isRunning = true;
        return true;
    }
    void doStop() override;
    void doIdle() override;

public:
    void getServiceInfo(::google::protobuf::RpcController* controller, const proto::ServiceInfoRequest* request,
                        proto::ServiceInfoResponse* response, ::google::protobuf::Closure* done) override;

    void createSnapshot(::google::protobuf::RpcController* controller, const proto::CreateSnapshotRequest* request,
                        proto::InformResponse* response, ::google::protobuf::Closure* done) override;
    void removeSnapshot(::google::protobuf::RpcController* controller, const proto::RemoveSnapshotRequest* request,
                        proto::InformResponse* response, ::google::protobuf::Closure* done) override;

private:
    bool _isRunning {false};
    std::unique_ptr<std::thread> _workThread;

private:
    BS_LOG_DECLARE();
};

class FakeMadroxService : public worker_framework::WorkerBase, public ::madrox::proto::MasterService
{
public:
    FakeMadroxService() = default;
    ~FakeMadroxService();

public:
    bool isRunning() const { return _isRunning; }
    bool startServer();
    bool workLoop();

public:
    bool doInit() override;
    bool doStart() override
    {
        _isRunning = true;
        return true;
    }
    void doStop() override;
    void doIdle() override;

public:
    void update(::google::protobuf::RpcController* controller, const ::madrox::proto::UpdateRequest* request,
                ::madrox::proto::UpdateResponse* response, ::google::protobuf::Closure* done) override;

    void getStatus(::google::protobuf::RpcController* controller, const ::madrox::proto::GetStatusRequest* request,
                   ::madrox::proto::GetStatusResponse* response, ::google::protobuf::Closure* done) override;

private:
    bool _isRunning {false};
    std::unique_ptr<std::thread> _workThread;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::task_base
