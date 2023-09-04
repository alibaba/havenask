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
#include "suez/admin/AdminWorker.h"

#include "aios/network/http_arpc/HTTPRPCServer.h"
#include "aios/network/http_arpc/ProtoJsonizer.h"
#include "aios/network/http_arpc/SimpleProtoJsonizer.h"
#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/OptionParser.h"
#include "compatible/RoleManagerWrapperImpl.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "suez/admin/AdminOps.h"
#include "suez/admin/AdminServiceImpl.h"
#include "suez/admin/SuezProtoJsonizer.h"
#include "suez/sdk/PathDefine.h"
#include "worker_framework/ZkState.h"

using namespace std;
using namespace autil;
using namespace cm_basic;

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, AdminWorker);

const size_t AdminWorker::RPC_SERVICE_THREAD_NUM = 4;
const size_t AdminWorker::RPC_SERVICE_QUEUE_SIZE = 32;
const string AdminWorker::RPC_SERVICE_THEAD_POOL_NAME = "suez_admin_tp";
const uint32_t AdminWorker::ADMIN_DEFAULT_AMONITOR_AGENT_PORT = 10086;

AdminWorker::AdminWorker()
    : _leaseInSecond(LeaderedWorkerBase::LEASE_IN_SECOND)
    , _leaseInLoopInterval(LeaderedWorkerBase::LOOP_INTERVAL)
    , _amonitorPort(ADMIN_DEFAULT_AMONITOR_AGENT_PORT) {
    _roleManager.reset(new carbon::compatible::RoleManagerWrapperImpl);
}

AdminWorker::~AdminWorker() {}

void AdminWorker::doInitOptions() {
    getOptionParser().addOption("-a", "--appId", "appId", OptionParser::OPT_STRING, false);
    getOptionParser().addOption("-b", "--hippoZk", "hippoZk", OptionParser::OPT_STRING, false);
    getOptionParser().addOption("-z", "--zkPath", "zkPath", OptionParser::OPT_STRING, true);
    getOptionParser().addOption("", "--leaseInSecond", "leaseInSecond", OptionParser::OPT_INT32, false);
    getOptionParser().addOption("", "--leaseInLoopInterval", "leaseInLoopInterval", OptionParser::OPT_INT32, false);
    getOptionParser().addOption("", "--amonitorPort", "amonitorPort", ADMIN_DEFAULT_AMONITOR_AGENT_PORT);
    getOptionParser().addOption("-s", "--storeUri", "storeUri", OptionParser::OPT_STRING, false);
    getOptionParser().addOption("", "--arpcPort", "arpcPort", OptionParser::OPT_UINT32, false);
    getOptionParser().addOption("", "--httpPort", "httpPort", OptionParser::OPT_UINT32, false);
    getOptionParser().addOption("-o", "--opsMode", "opsMode", OptionParser::STORE_TRUE);
    getOptionParser().addOption("", "--localCarbon", "localCarbon", OptionParser::STORE_TRUE);
    getOptionParser().addOption("", "--localCm2Mode", "localCm2Mode", OptionParser::STORE_TRUE);
    getOptionParser().addMultiValueOption("", "--env", "env");
}

bool AdminWorker::doInit() {
    getOptionParser().getOptionValue("appId", _appId);
    getOptionParser().getOptionValue("hippoZk", _hippoZk);
    getOptionParser().getOptionValue("zkPath", _zkPath);
    getOptionParser().getOptionValue("leaseInSecond", _leaseInSecond);
    getOptionParser().getOptionValue("leaseInLoopInterval", _leaseInLoopInterval);
    getOptionParser().getOptionValue("amonitorPort", _amonitorPort);
    getOptionParser().getOptionValue("storeUri", _storeUri);
    getOptionParser().getOptionValue("arpcPort", _arpcPort);
    getOptionParser().getOptionValue("httpPort", _httpPort);
    getOptionParser().getOptionValue("opsMode", _opsMode);
    getOptionParser().getOptionValue("localCarbon", _localCarbon);
    getOptionParser().getOptionValue("localCm2Mode", _localCm2Mode);
    initEnvArgs();

    arpc::ThreadPoolDescriptor tpd =
        arpc::ThreadPoolDescriptor(RPC_SERVICE_THEAD_POOL_NAME, RPC_SERVICE_THREAD_NUM, RPC_SERVICE_QUEUE_SIZE);

    if (_arpcPort) {
        setPort(_arpcPort);
    }
    if (_httpPort) {
        setHTTPPort(_httpPort);
    }

    if (_opsMode) {
        AUTIL_LOG(INFO, "admin start with ops mode");
        _adminOps = std::make_unique<AdminOps>();
        if (!_adminOps->init(_storeUri, _localCarbon, _localCm2Mode, _roleManager)) {
            AUTIL_LOG(ERROR, "init admin ops failed");
            return false;
        }
        const auto &serviceList = _adminOps->listService();
        for (auto service : serviceList) {
            if (!registerService(service, tpd)) {
                return false;
            }
        }
    } else {
        AUTIL_LOG(INFO, "admin start without ops mode");

        _adminService = std::make_unique<AdminServiceImpl>();
        if (!registerService(_adminService.get(), tpd)) {
            string errMsg = "register admin service failed!";
            AUTIL_LOG(WARN, "[%s]", errMsg.c_str());
            return false;
        }
    }

    if (!_localCarbon) {
        AUTIL_LOG(INFO, "init carbon");
        ::google::protobuf::Service *carbonMasterService = _roleManager->getService();
        if (carbonMasterService) {
            if (!registerService(carbonMasterService, tpd)) {
                AUTIL_LOG(WARN, "register carbon master service failed!");
                return false;
            }
            AUTIL_LOG(INFO, "register carbon master service.");
        }

        ::google::protobuf::Service *carbonOpsService = _roleManager->getOpsService();
        if (carbonOpsService) {
            if (!registerService(carbonOpsService, tpd)) {
                AUTIL_LOG(WARN, "register carbon ops service failed!");
                return false;
            }
            AUTIL_LOG(INFO, "register carbon ops service.");
        }
    }

    if (!initHTTPRPCServer()) {
        AUTIL_LOG(WARN, "init http arpc server failed!");
        return false;
    }

    http_arpc::ProtoJsonizerPtr protoJsonizer(new SuezProtoJsonizer());
    http_arpc::HTTPRPCServer *httpRpcServer = getHTTPRPCServer();
    if (!_opsMode) {
        httpRpcServer->setProtoJsonizer(protoJsonizer);
    } else {
        httpRpcServer->setProtoJsonizer(std::make_shared<http_arpc::SimpleProtoJsonizer>());
    }

    AUTIL_LOG(INFO, "admin init success");

    return true;
}

void AdminWorker::initEnvArgs() {
    auto &optionParser = getOptionParser();
    vector<string> envArgs;
    if (!optionParser.getOptionValue("env", envArgs)) {
        return;
    }
    for (auto &envArg : envArgs) {
        size_t idx = envArg.find('=');
        if (idx == string::npos || idx == 0) {
            fprintf(stderr, "invalid env: %s\n", envArg.c_str());
            return;
        }
        string key = envArg.substr(0, idx);
        string value = envArg.substr(idx + 1);
        if (!autil::EnvUtil::setEnv(key, value, true)) {
            fprintf(stderr, "export env %s failed\n", envArg.c_str());
            return;
        }
    }
}

bool AdminWorker::doStart() {
    AUTIL_LOG(INFO, "admin worker starting ...");
    if (!initLeaderElector(PathDefine::getAdminZkRoot(_zkPath), _leaseInSecond, _leaseInLoopInterval, "")) {
        AUTIL_LOG(WARN, "Admin init leader checker failed, zkRoot[%s]", _zkPath.c_str());
        return false;
    }
    if (!waitToBeLeader()) {
        AUTIL_LOG(WARN, "failed to be leader!");
        return false;
    }

    bool isNewStart = false;
    if (!checkIsNewStart(isNewStart)) {
        AUTIL_LOG(ERROR, "check is new start fail");
        return false;
    }

    if (isNewStart) {
        AUTIL_LOG(INFO, "carbon is new started.");
    } else {
        AUTIL_LOG(INFO, "carbon is not new started, it will be recover.");
    }

    if (!_localCarbon) {
        if (!_roleManager->init(_appId, _hippoZk, _zkPath, isNewStart, getLeaderElector(), _amonitorPort)) {
            AUTIL_LOG(ERROR, "init role manager fail");
            return false;
        }
        if (isNewStart && !deleteNewStartTag()) {
            return false;
        }
        if (!_roleManager->start()) {
            AUTIL_LOG(ERROR, "start role manager fail");
            return false;
        }
    }

    if (_opsMode) {
        AUTIL_LOG(INFO, "start admin with ops mode");
        if (!_adminOps->start()) {
            AUTIL_LOG(ERROR, "start admim ops failed");
            return false;
        }
    } else {
        string statusFileName = PathDefine::getAdminSerializeFile(_zkPath);
        AUTIL_LOG(INFO, "admin status file is %s", statusFileName.c_str());
        auto state = std::make_unique<worker_framework::ZkState>(getZkWrapper(), statusFileName);
        if (!_adminService->start(isNewStart, _roleManager, std::move(state))) {
            AUTIL_LOG(ERROR, "start admin service failed");
            return false;
        }
    }

    AUTIL_LOG(INFO, "admin worker started, arpc port [%u], http port [%u]", getPort(), getHTTPPort());
    return true;
}

void AdminWorker::doStop() {
    if (_adminService) {
        _adminService->stop();
    }
    if (_adminOps) {
        _adminOps->stop();
    }
    stopRPCServer();
    _roleManager->stop();
    shutdownLeaderElector();
}

void AdminWorker::noLongerLeader() {
    const string &address = getAddress();
    AUTIL_LOG(WARN, "AdminWorker[%s] is no longer leader, exit now", address.c_str());
    stop();
}

bool AdminWorker::checkIsNewStart(bool &isNewStart) {
    isNewStart = false;
    string newStartFilePath = PathDefine::getIsNewStartPath(_zkPath);
    fslib::ErrorCode ec = fslib::fs::FileSystem::isExist(newStartFilePath);
    if (ec != fslib::EC_TRUE && ec != fslib::EC_FALSE) {
        AUTIL_LOG(ERROR,
                  "check simple master new start failed. "
                  "new start tag file:[%s], error: [%s]",
                  newStartFilePath.c_str(),
                  fslib::fs::FileSystem::getErrorString(ec).c_str());
        return false;
    } else if (ec == fslib::EC_TRUE) {
        isNewStart = true;
    }
    return true;
}

bool AdminWorker::deleteNewStartTag() {
    string newStartFilePath = PathDefine::getIsNewStartPath(_zkPath);
    if (fslib::fs::FileSystem::remove(newStartFilePath) == fslib::EC_OK) {
        AUTIL_LOG(INFO, "delete newStart tag file [%s] success!", newStartFilePath.c_str());
        return true;
    } else {
        AUTIL_LOG(ERROR, "delete newStart tag file [%s] failed!", newStartFilePath.c_str());
        return false;
    }
}

} // namespace suez

namespace worker_framework {
;
WorkerBase *createWorker() { return new suez::AdminWorker; }
}; // namespace worker_framework
