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
#include "swift/admin/AdminWorker.h"

#include <arpc/ThreadPoolDescriptor.h>
#include <functional>
#include <iostream>
#include <memory>
#include <stdio.h>
#include <unistd.h>

#include "arpc/ANetRPCServer.h"
#include "autil/CommonMacros.h"
#include "autil/EnvUtil.h"
#include "autil/OptionParser.h"
#include "autil/StringUtil.h"
#include "autil/ThreadPool.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "kmonitor/client/KMonitorFactory.h"
#include "kmonitor/client/common/Util.h"
#include "kmonitor/client/core/MetricsConfig.h"
#include "swift/admin/AdminServiceImpl.h"
#include "swift/admin/SysController.h"
#include "swift/common/Common.h"
#include "swift/common/PathDefine.h"
#include "swift/config/AdminConfig.h"
#include "swift/heartbeat/HeartbeatServiceImpl.h"
#include "swift/heartbeat/ZkHeartbeatWrapper.h"
#include "swift/monitor/AdminMetricsReporter.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "worker_framework/LeaderElector.h"

using namespace swift::common;
using namespace swift::protocol;
using namespace swift::monitor;
using namespace swift::config;

using namespace std;
using namespace autil;
using namespace kmonitor;

namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, AdminWorker);

const int64_t AdminWorker::AMON_REPORT_INTERVAL = 1000 * 1000; // 1 second

AdminWorker::AdminWorker() {}

AdminWorker::~AdminWorker() {
    _amonitorThreadPtr.reset();
    DELETE_AND_SET_NULL(_adminServiceImpl);
    DELETE_AND_SET_NULL(_heartbeatServiceImpl);
    DELETE_AND_SET_NULL(_sysController);
    DELETE_AND_SET_NULL(_adminConfig);
    DELETE_AND_SET_NULL(_adminMetricsReporter);
}

void AdminWorker::doInitOptions() {
    auto &optionParser = getOptionParser();
    optionParser.addOption("-c", "--config", "config", OptionParser::OPT_STRING, true);
    optionParser.addMultiValueOption("", "--env", "env");
}

void AdminWorker::doExtractOptions() {
    getOptionParser().getOptionValue("config", _configPath);
    initEnvArgs();
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
            AUTIL_LOG(ERROR, "invalid env: %s\n", envArg.c_str());
            return;
        }
        string key = envArg.substr(0, idx);
        string value = envArg.substr(idx + 1);
        if (!autil::EnvUtil::setEnv(key, value, true)) {
            AUTIL_LOG(ERROR, "export env %s failed\n", envArg.c_str());
            return;
        }
    }
}

bool AdminWorker::initMonitor() {
    string slaveIp = autil::EnvUtil::getEnv("HIPPO_SLAVE_IP", "127.0.0.1");
    string instance = autil::EnvUtil::getEnv("HIPPO_APP", "unknown");
    string port = autil::EnvUtil::getEnv("kmonitorPort", "4141");
    string serviceName = autil::EnvUtil::getEnv("kmonitorServiceName", instance);
    string sinkAddress = autil::EnvUtil::getEnv("kmonitorSinkAddress", slaveIp);
    string tenant = autil::EnvUtil::getEnv("kmonitorTenant", "swift");
    string host;
    kmonitor::Util::GetHostIP(host);
    MetricsConfig metricsConfig;
    metricsConfig.set_tenant_name(tenant);
    metricsConfig.set_service_name(serviceName);
    metricsConfig.set_sink_address(sinkAddress + ":" + port);
    metricsConfig.set_use_common_tag(false);
    metricsConfig.set_inited(true);
    metricsConfig.AddGlobalTag("instance", instance);
    metricsConfig.AddGlobalTag("host", host);
    bool enableKmonLogFileSink = false, enableKmonPromSink = false;
    enableKmonLogFileSink = autil::EnvUtil::getEnv("kmonitorEnableLogFileSink", enableKmonLogFileSink);
    enableKmonPromSink = autil::EnvUtil::getEnv("kmonitorEnablePrometheusSink", enableKmonPromSink);
    metricsConfig.set_enable_log_file_sink(enableKmonLogFileSink);
    metricsConfig.set_enable_prometheus_sink(enableKmonPromSink);

    // alog has not init yet
    cout << "init kmonitor with[" << ToJsonString(metricsConfig, true) << "]" << endl;
    if (!KMonitorFactory::Init(metricsConfig)) {
        fprintf(stderr, "init kmonitor factory failed with[%s]", ToJsonString(metricsConfig, true).c_str());
        return false;
    }
    KMonitorFactory::Start();
    KMonitorFactory::registerBuildInMetrics(nullptr, "swift.worker.admin");
    _adminMetricsReporter = new AdminMetricsReporter();
    return true;
}

bool AdminWorker::doInit() {
    _adminConfig = AdminConfig::loadConfig(_configPath);
    if (!_adminConfig) {
        AUTIL_LOG(WARN, "admin config [%s] load fail", _configPath.c_str());
        return false;
    }
    _adminConfig->setWorkDir(getCwdPath());
    _adminConfig->setLogConfFile(getLogConfFile());

    AUTIL_LOG(INFO, "admin config[%s]", _adminConfig->getConfigStr().c_str());
    if (!initSysController()) {
        AUTIL_LOG(ERROR, "Failed to init sys controller.");
        return false;
    }
    if (!initAdminService()) {
        AUTIL_LOG(ERROR, "Failed to init admin service.");
        return false;
    }
    if (!initHeartbeatService()) {
        AUTIL_LOG(ERROR, "Failed to init heartbeat service.");
        return false;
    }
    if (!initHTTPRPCServer(_adminConfig->getAdminHttpThreadNum(), _adminConfig->getAdminHttpQueueSize())) {
        AUTIL_LOG(WARN, "init http arpc server failed!");
        return false;
    }
    uint32_t rpcPort = 0, httpPort = 0;
    if (_adminConfig->getUseRecommendPort() && PathDefine::readRecommendPort(rpcPort, httpPort)) {
        AUTIL_LOG(INFO, "recommend rpc port[%d], http port[%d]", rpcPort, httpPort);
        setPort(rpcPort);
        setHTTPPort(httpPort);
    }
    return true;
}

bool AdminWorker::doStart() {
    AUTIL_LOG(INFO, "admin worker starting ...");
    string path = common::PathDefine::getAdminLockDir(_adminConfig->getZkRoot());
    int64_t leaderLeaseTimeMs = _adminConfig->getLeaderLeaseTime() * 1000;
    int64_t leaderLoopIntervalMs = _adminConfig->getLeaderLoopInterval() * 1000;
    string accKey = getAccessKey();
    if (!initLeaderElectorNotStart(path, leaderLeaseTimeMs, leaderLoopIntervalMs, accKey)) {
        AUTIL_LOG(WARN, "admin init leader elector failed, zkRoot[%s]", _adminConfig->getZkRoot().c_str());
        return false;
    }
    getLeaderElector()->setForbidCampaignLeaderTimeMs(_adminConfig->getForbidCampaignTimeMs());
    if (!initHeartbeat()) {
        return false;
    }
    if (!_sysController->start(getZkWrapper(), getLeaderElector())) {
        AUTIL_LOG(WARN, "start SysController failed!");
        return false;
    }
    if (!startLeaderElector()) {
        AUTIL_LOG(WARN, "admin start leader elector failed, zkRoot[%s]", _adminConfig->getZkRoot().c_str());
        return false;
    }
    _amonitorThreadPtr = autil::LoopThread::createLoopThread(
        std::bind(&AdminWorker::amonReport, this), AMON_REPORT_INTERVAL, "amon_report");
    PathDefine::writeRecommendPort(getPort(), getHTTPPort());
    AUTIL_LOG(INFO, "admin worker started, arpc port[%u], http port[%u]", getPort(), getHTTPPort());
    return true;
}

void AdminWorker::becomeLeader() {
    // since callback will be called before SysController started
    if (_sysController == NULL || _sysController->isStopped()) {
        AUTIL_LOG(ERROR, "system controller is stopped!");
        return;
    }
    if (!_sysController->start(getZkWrapper(), getLeaderElector())) {
        AUTIL_LOG(WARN, "start SysController failed!");
        stop();
    } else if (_sysController->turnToLeader(getAddress(), getHTTPAddress())) {
        AUTIL_LOG(INFO, "becomes leader arpc [%s], http [%s]", getAddress().c_str(), getHTTPAddress().c_str());
    } else {
        AUTIL_LOG(WARN, "%s failed to become leader.", getAddress().c_str());
        stop(); // stop worker, worker will restart later by hippo
    }
}

void AdminWorker::noLongerLeader() {
    ScopedLock lock(_exitMutex);
    if (_sysController == NULL || _sysController->isStopped()) {
        AUTIL_LOG(ERROR, "system controller is stop!");
        return;
    }
    _sysController->turnToFollower();
    AUTIL_LOG(INFO, "becomes follower arpc [%s], http [%s].", getAddress().c_str(), getHTTPAddress().c_str());
    // TODO: need to think, maybe not stop
    stop(); // stop current admin worker
}

void AdminWorker::doStop() {
    AUTIL_LOG(INFO, "admin worker is stopping.");
    _amonitorThreadPtr.reset();
    stopRPCServer();
    if (_sysController) {
        _sysController->stop();
    }
    shutdownLeaderElector();
    DELETE_AND_SET_NULL(_adminServiceImpl);
    DELETE_AND_SET_NULL(_heartbeatServiceImpl);
    DELETE_AND_SET_NULL(_sysController);
    AUTIL_LOG(INFO, "admin worker is stopped.");
}

void AdminWorker::doIdle() {
    heartbeat();
    if (_sysController->isStopped()) {
        stop();
    }
}

string AdminWorker::getAccessKey() {
    string key = getAddress() + "_" + StringUtil::toString(getpid());
    return key;
}

string AdminWorker::getWorkerAddress() { return PathDefine::getRoleAddress("admin", getIp(), (uint16_t)(getPort())); }

string AdminWorker::getHeartbeatAddress() {
    return PathDefine::heartbeatReportAddress(_adminConfig->getZkRoot(), getWorkerAddress());
}

protocol::HeartbeatInfo AdminWorker::getHeartbeatInfo() {
    protocol::HeartbeatInfo info;
    info.set_role(protocol::ROLE_TYPE_ADMIN);
    return info;
}

bool AdminWorker::initHeartbeat() {
    string address = getHeartbeatAddress();
    if (!_heartbeatReporter.setParameter(address)) {
        AUTIL_LOG(ERROR, "HeartBeatReporter setParameter error. [%s]", address.c_str());
        return false;
    }
    return true;
}

void AdminWorker::heartbeat() {
    protocol::HeartbeatInfo info = getHeartbeatInfo();
    info.set_address(getWorkerAddress());
    info.set_alive(true);
    _heartbeatReporter.heartBeat(info);
}

void AdminWorker::amonReport() {
    if (_sysController) {
        _sysController->reportMetrics();
    }
    arpc::ANetRPCServer *rpcServer = getRPCServer();
    size_t queueLen = -1;
    if (rpcServer) {
        queueLen = rpcServer->GetThreadPool()->getItemCount();
    }
    _adminMetricsReporter->reportAdminQueueSize(queueLen);
}

bool AdminWorker::initSysController() {
    _sysController = new SysController(_adminConfig, _adminMetricsReporter);
    if (!_sysController->init()) {
        AUTIL_LOG(ERROR, "Failed to init sys controller.");
        DELETE_AND_SET_NULL(_sysController);
        return false;
    }
    return true;
}

bool AdminWorker::initAdminService() {
    _adminServiceImpl = new AdminServiceImpl(_adminConfig, _sysController, _adminMetricsReporter);
    if (!_sysController || !_adminServiceImpl->init()) {
        AUTIL_LOG(ERROR, "Failed to init admin service impl.");
        return false;
    }
    if (!registerService(_adminServiceImpl)) {
        AUTIL_LOG(ERROR, "admin arpc server register service fail.");
        return false;
    }
    return true;
}

bool AdminWorker::initHeartbeatService() {
    _heartbeatServiceImpl = new heartbeat::HeartbeatServiceImpl(_adminConfig, _sysController, _adminMetricsReporter);
    if (!_sysController || !_heartbeatServiceImpl->init()) {
        AUTIL_LOG(ERROR, "Failed to init heartbeat service.");
        return false;
    }
    arpc::ThreadPoolDescriptor tpd(
        "HeartbeatArpcTd", _adminConfig->getHeartbeatThreadNum(), _adminConfig->getHeartbeatQueueSize());
    if (!registerService(_heartbeatServiceImpl, tpd)) {
        AUTIL_LOG(ERROR, "admin arpc server register heartbeat service fail.");
        return false;
    }
    return true;
}

} // namespace admin
} // namespace swift
