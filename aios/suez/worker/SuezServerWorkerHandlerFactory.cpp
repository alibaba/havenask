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
#include "suez/worker/SuezServerWorkerHandlerFactory.h"

#include <memory>
#include <sys/sysinfo.h>

#include "aios/network/arpc/arpc/ANetRPCServer.h"
#include "aios/network/gig/multi_call/agent/GigAgent.h"
#include "aios/network/gig/multi_call/rpc/GigRpcServer.h"

#include "fslib/fs/FileSystem.h"
#include "fslib/util/FileUtil.h"
#include "suez/heartbeat/HeartbeatManager.h"
#include "suez/sdk/SearchManager.h"
#include "suez/worker/DebugServiceImpl.h"
#include "suez/worker/EnvParam.h"
#include "suez/worker/KMonitorManager.h"
#include "suez/worker/SelfKillerService.h"

using namespace std;
using namespace autil;
using namespace multi_call;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, SuezServerWorkerHandlerFactory);

SuezServerWorkerHandlerFactory::SuezServerWorkerHandlerFactory(
    const std::shared_ptr<multi_call::GigRpcServer> &rpcServer, const std::string &installRoot)
    : _rpcServer(rpcServer), _installRoot(installRoot) {}

SuezServerWorkerHandlerFactory::~SuezServerWorkerHandlerFactory() {
    AUTIL_LOG(INFO, "suez_worker stop begin");

    release();
    // TaskExecutor holds RpcService lifetime, Rpc Service dtor after Rpc Server dtor
    _taskExecutor.reset();
    _hbManager.reset(); // hb should reset after http service, hb has arpc call
    _selfKiller.reset();
    fslib::fs::FileSystem::close(); // unload fslib plugin
    _kmonManager.reset();
    AUTIL_LOG(INFO, "suez_worker stop end");
}

void SuezServerWorkerHandlerFactory::release() {
    AUTIL_LOG(INFO, "suez_worker release begin");
    if (_reportThread) {
        _reportThread->stop();
        _reportThread.reset();
    }
    if (_taskExecutor) {
        _taskExecutor->release();
    }
    if (_rpcServer) {
        _rpcServer.reset();
    }
    AUTIL_LOG(INFO, "suez_worker release end");
}

bool SuezServerWorkerHandlerFactory::initilize(const EnvParam &param) {
    waitForDebugger(param);
    _kmonManager.reset(new KMonitorManager());
    if (!_kmonManager->init(param)) {
        AUTIL_LOG(INFO, "kmonitor init failed");
        return false;
    }
    if (!startRdma(param)) {
        return false;
    }
    if (!startHttp(param)) {
        return false;
    }
    if (!startGigRpcServer(param)) {
        return false;
    }
    _rpcServerWrapper.arpcServer = _rpcServer->getArpcServer();
    _rpcServerWrapper.httpRpcServer = _rpcServer->getHttpArpcServer();
    _rpcServerWrapper.gigRpcServer = _rpcServer.get();

    if (param.debugMode) {
        if (!initTaskExecutor(param)) {
            return false;
        }
        _debugService = std::make_shared<DebugServiceImpl>();
        if (!_debugService->init(&_rpcServerWrapper, _taskExecutor)) {
            AUTIL_LOG(ERROR, "register debug service failed");
            return false;
        }
    } else {
        _hbManager.reset(new HeartbeatManager());
        if (!_hbManager->init(
                &_rpcServerWrapper, param.carbonIdentifier, param.procInstanceId, param.packageCheckSum)) {
            AUTIL_LOG(ERROR, "register hb service failed");
            return false;
        }
        if (!initTaskExecutor(param)) {
            return false;
        }
    }
    _selfKiller.reset(new SelfKillerService());
    if (!_selfKiller->init(&_rpcServerWrapper)) {
        AUTIL_LOG(ERROR, "register self killer service failed");
        return false;
    }
    if (!_kmonManager->postInit(param, &_rpcServerWrapper)) {
        AUTIL_LOG(INFO, "kmonitor post init failed");
        return false;
    }
    _reportThread = LoopThread::createLoopThread(
        std::bind(&SuezServerWorkerHandlerFactory::report, this), param.reportLoopInterval, "SuezReport");
    if (!_reportThread) {
        AUTIL_LOG(ERROR, "start report thread failed");
        return false;
    }
    AUTIL_LOG(INFO, "initilize succeed");
    return true;
}

bool SuezServerWorkerHandlerFactory::startHttp(const EnvParam &param) {
    multi_call::HttpArpcServerDescription desc;
    desc.port = param.httpPort;
    desc.name = "SuezHttp";
    desc.ioThreadNum = param.httpIoThreadNum;
    desc.threadNum = param.httpThreadNum;
    desc.queueSize = param.httpQueueSize;
    desc.decodeUri = param.decodeUri;
    desc.haCompatible = param.haCompatible;
    return _rpcServer->startHttpArpcServer(desc);
}

bool SuezServerWorkerHandlerFactory::startRdma(const EnvParam &param) {
    if (param.rdmaPort.empty()) {
        return true;
    }
    multi_call::RdmaArpcServerDescription desc;
    desc.name = "SuezRdma";
    desc.port = param.rdmaPort;
    desc.arpcWorkerThreadNum = param.rdmaRpcThreadNum;
    desc.arpcWorkerQueueSize = param.rdmaRpcQueueSize;
    desc.rdmaIoThreadNum = param.rdmaIoThreadNum;
    desc.rdmaWorkerThreadNum = param.rdmaWorkerThreadNum;
    return _rpcServer->startRdmaArpcServer(desc);
}

bool SuezServerWorkerHandlerFactory::startGigRpcServer(const EnvParam &param) {
    if (!initGrpcServer(param)) {
        return false;
    }
    auto gigAgent = _rpcServer->getAgent();
    assert(gigAgent);
    return gigAgent->init("", param.gigEnableAgentStat);
}

bool SuezServerWorkerHandlerFactory::initGrpcServer(const EnvParam &param) {
    if (param.gigGrpcPort.empty()) {
        return true;
    }
    // gig grpc
    multi_call::GrpcServerDescription grpcDesc;
    if (!getGigGrpcDesc(param, "server", grpcDesc)) {
        AUTIL_LOG(ERROR, "failed to get gig grpc description.");
        return false;
    }
    if (!_rpcServer->initGrpcServer(grpcDesc)) {
        AUTIL_LOG(ERROR, "initGrpcServer failed.");
        return false;
    }
    AUTIL_LOG(INFO, "initGrpcServer success, grpc listen port [%d]", _rpcServer->getGrpcPort());
    return true;
}

bool SuezServerWorkerHandlerFactory::getGigGrpcDesc(const EnvParam &param,
                                                    const string &role,
                                                    multi_call::GrpcServerDescription &desc) const {
    desc.ioThreadNum = param.gigGrpcThreadNum;
    desc.port = param.gigGrpcPort;
    if (param.grpcCertsDir.empty()) {
        return true;
    }
    auto &secureConfig = desc.secureConfig;
    const auto &rootDir = param.grpcCertsDir;
    auto rootCertsFile = fslib::util::FileUtil::joinFilePath(rootDir, "ca.crt");
    if (!fslib::util::FileUtil::readFile(rootCertsFile, secureConfig.pemRootCerts)) {
        AUTIL_LOG(ERROR, "get file content failed: [%s]", rootCertsFile.c_str());
        return false;
    }
    if (secureConfig.pemRootCerts.empty()) {
        AUTIL_LOG(ERROR, "root certs is empty");
        return false;
    }
    auto serverPrivateKey = fslib::util::FileUtil::joinFilePath(rootDir, role + ".key");
    if (!fslib::util::FileUtil::readFile(serverPrivateKey, secureConfig.pemPrivateKey)) {
        AUTIL_LOG(ERROR, "get file content failed: [%s]", serverPrivateKey.c_str());
        return false;
    }
    auto serverCert = fslib::util::FileUtil::joinFilePath(rootDir, role + ".crt");
    if (!fslib::util::FileUtil::readFile(serverCert, secureConfig.pemCertChain)) {
        AUTIL_LOG(ERROR, "get file content failed: [%s]", serverCert.c_str());
        return false;
    }
    secureConfig.targetName = param.grpcTargetName;
    AUTIL_LOG(INFO, "read %s tls config success", role.c_str());
    return true;
}

void SuezServerWorkerHandlerFactory::waitForDebugger(const EnvParam &param) {
    if (param.waitForDebugger) {
        static volatile bool loopFlag = true;
        while (loopFlag) {
            AUTIL_LOG(WARN, "wait for gdb to attach, bool variable address [%p]", &loopFlag);
            usleep(2000 * 1000);
        }
        AUTIL_LOG(INFO, "loopFlag changed, continue");
    }
    AUTIL_LOG(INFO, "wait for debugger finished");
}

bool SuezServerWorkerHandlerFactory::initTaskExecutor(const EnvParam &param) {
    _taskExecutor.reset(new TaskExecutor(&_rpcServerWrapper, _kmonManager->getMetaInfo()));
    _taskExecutor->setAllowPartialTableReady(param.allowPartialTableReady);
    _taskExecutor->setNeedShutdownGracefully(param.needShutdownGracefully);
    _taskExecutor->setNoDiskQuotaCheck(param.noDiskQuotaCheck);
    if (!_taskExecutor->start(_hbManager.get(), _installRoot, param)) {
        AUTIL_LOG(ERROR, "start TaskExecutor");
        return false;
    }
    return true;
}

void SuezServerWorkerHandlerFactory::report() {
    if (_taskExecutor) {
        _taskExecutor->reportMetrics();
    }
}

} // namespace suez
