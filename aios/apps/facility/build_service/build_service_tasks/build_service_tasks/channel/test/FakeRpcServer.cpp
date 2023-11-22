#include "build_service_tasks/channel/test/FakeRpcServer.h"

#include <arpc/ThreadPoolDescriptor.h>
#include <iosfwd>
#include <stdint.h>
#include <string>
#include <unistd.h>

#include "alog/Logger.h"

using namespace std;
using namespace build_service::task_base;

BS_LOG_SETUP(task_base, FakeBsAdmin);
BS_LOG_SETUP(task_base, FakeMadroxService);

FakeBsAdmin::~FakeBsAdmin()
{
    if (_isRunning) {
        stop();
    }
    if (_workThread) {
        _workThread->join();
    }
}

bool FakeBsAdmin::doInit()
{
    arpc::ThreadPoolDescriptor tpd;
    if (!registerService(this, tpd)) {
        BS_LOG(ERROR, "register service failed!");
        return false;
    }
    return true;
}

bool FakeBsAdmin::startServer()
{
    _workThread = make_unique<thread>(&FakeBsAdmin::workLoop, this);
    uint32_t retryLimit = 10;
    uint32_t retryCounter = 0;
    while (_isRunning == false) {
        if (retryCounter >= retryLimit) {
            BS_LOG(ERROR, "reach retryLimit[%u], server stop", retryLimit);
            stop();
            return false;
        }
        sleep(2u);
        retryCounter++;
    }
    BS_LOG(INFO, "bs start server success");
    return true;
}

void FakeBsAdmin::doStop()
{
    stopRPCServer();
    _isRunning = false;
    BS_LOG(INFO, "bs server stopped");
}

void FakeBsAdmin::doIdle() {}

bool FakeBsAdmin::workLoop() { return run(); }

void FakeBsAdmin::getServiceInfo(::google::protobuf::RpcController* controller,
                                 const proto::ServiceInfoRequest* request, proto::ServiceInfoResponse* response,
                                 ::google::protobuf::Closure* done)
{
    BS_LOG(INFO, "gs request[%s]", request->ShortDebugString().c_str());
    done->Run();
}

void FakeBsAdmin::createSnapshot(::google::protobuf::RpcController* controller,
                                 const proto::CreateSnapshotRequest* request, proto::InformResponse* response,
                                 ::google::protobuf::Closure* done)
{
    BS_LOG(INFO, "createSnapshot request[%s]", request->ShortDebugString().c_str());
    done->Run();
}

void FakeBsAdmin::removeSnapshot(::google::protobuf::RpcController* controller,
                                 const proto::RemoveSnapshotRequest* request, proto::InformResponse* response,
                                 ::google::protobuf::Closure* done)
{
    BS_LOG(INFO, "removeSnapshot request[%s]", request->ShortDebugString().c_str());
    done->Run();
}

FakeMadroxService::~FakeMadroxService()
{
    if (_isRunning) {
        stop();
    }
    if (_workThread) {
        _workThread->join();
    }
}

bool FakeMadroxService::doInit()
{
    arpc::ThreadPoolDescriptor tpd;
    if (!registerService(this, tpd)) {
        BS_LOG(ERROR, "madorx register service failed!");
        return false;
    }
    return true;
}

bool FakeMadroxService::startServer()
{
    _workThread = make_unique<thread>(&FakeMadroxService::workLoop, this);
    uint32_t retryLimit = 10;
    uint32_t retryCounter = 0;
    while (_isRunning == false) {
        if (retryCounter >= retryLimit) {
            BS_LOG(ERROR, "reach retryLimit[%u], server stop", retryLimit);
            stop();
            return false;
        }
        sleep(2u);
        retryCounter++;
    }
    BS_LOG(INFO, "madrox start server success");
    return true;
}

void FakeMadroxService::doStop()
{
    stopRPCServer();
    _isRunning = false;
    BS_LOG(INFO, "madrox server stopped");
}

void FakeMadroxService::doIdle() {}

bool FakeMadroxService::workLoop() { return run(); }

void FakeMadroxService::update(::google::protobuf::RpcController* controller,
                               const ::madrox::proto::UpdateRequest* request, ::madrox::proto::UpdateResponse* response,
                               ::google::protobuf::Closure* done)
{
    BS_LOG(INFO, "madrox update request[%s]", request->ShortDebugString().c_str());
    response->mutable_errorinfo()->set_errorcode(::madrox::proto::MASTER_ERROR_NONE);
    response->mutable_errorinfo()->set_errormsg("");
    done->Run();
}

void FakeMadroxService::getStatus(::google::protobuf::RpcController* controller,
                                  const ::madrox::proto::GetStatusRequest* request,
                                  ::madrox::proto::GetStatusResponse* response, ::google::protobuf::Closure* done)
{
    BS_LOG(INFO, "madrox getStatus request[%s]", request->ShortDebugString().c_str());
    response->mutable_errorinfo()->set_errorcode(::madrox::proto::MASTER_ERROR_NONE);
    response->mutable_errorinfo()->set_errormsg("");
    response->set_status(::madrox::proto::DONE);
    done->Run();
}
