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
#include "swift/broker/service/BrokerWorker.h"

#include <algorithm>
#include <functional>
#include <iostream>
#include <map>
#include <new>
#include <stdio.h>
#include <unistd.h>
#include <unordered_map>
#include <utility>

#include "arpc/ANetRPCServer.h"
#include "autil/CommonMacros.h"
#include "autil/EnvUtil.h"
#include "autil/HashAlgorithm.h"
#include "autil/OptionParser.h"
#include "autil/StringUtil.h"
#include "autil/ThreadPool.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/metric/Cpu.h"
#include "autil/metric/ProcessMemory.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "kmonitor/client/KMonitorFactory.h"
#include "kmonitor/client/common/Util.h"
#include "kmonitor/client/core/MetricsConfig.h"
#include "swift/broker/service/PartitionStatusUpdater.h"
#include "swift/broker/service/TopicPartitionSupervisor.h"
#include "swift/broker/service/TransporterImpl.h"
#include "swift/common/PathDefine.h"
#include "swift/config/AuthorizerInfo.h"
#include "swift/config/BrokerConfig.h"
#include "swift/config/BrokerConfigParser.h"
#include "swift/config/ClientAuthorizerInfo.h"
#include "swift/config/ConfigDefine.h"
#include "swift/heartbeat/TaskInfoMonitor.h"
#include "swift/heartbeat/ZkHeartbeatWrapper.h"
#include "swift/monitor/BrokerMetricsReporter.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "swift/util/IpMapper.h"
#include "swift/util/PanguInlineFileUtil.h"
#include "swift/util/ZkDataAccessor.h"
#include "swift/version.h"

using namespace swift::common;
using namespace swift::monitor;
using namespace swift::util;
using namespace swift::config;
using namespace swift::protocol;
using namespace swift::network;
using namespace swift::auth;
using namespace swift::heartbeat;

using namespace std;
using namespace autil;
using namespace kmonitor;
using namespace fslib::fs;

namespace swift {
namespace service {
AUTIL_LOG_SETUP(swift, BrokerWorker);

const int64_t BrokerWorker::CHECK_MONITOR_INTERVAL = 200 * 1000;      // 200ms
const int64_t BrokerWorker::AMON_REPORT_INTERVAL = 1000 * 1000;       // 1s
const uint32_t BrokerWorker::LEADER_RETRY_TIME_INTERVAL = 100 * 1000; // 100ms
const uint32_t BrokerWorker::LEADER_RETRY_COUNT = 1200;

const string BrokerWorker::CONFIG = "config";
const string BrokerWorker::ROLE = "role";

const string BrokerWorker::DEFAULT_LEADER_LEASE_TIME = "10";   // 10s
const string BrokerWorker::DEFAULT_LEADER_LOOP_INTERVAL = "2"; // 2s

BrokerWorker::BrokerWorker()
    : _transporterImpl(NULL)
    , _tpSupervisor(NULL)
    , _statusUpdater(NULL)
    , _brokerConfig(NULL)
    , _ipMapper(NULL)
    , _taskInfoMonitor(NULL)
    , _metricsReporter(NULL)
    , _sessionId(0)
    , _cpu(NULL)
    , _mem(NULL)
    , _channelManager(NULL)
    , _adminAdapter(NULL)
    , _brokerHealthChecker(NULL) {
    _brokerVersionInfo.set_version(swift_broker_version_str);
    _brokerVersionInfo.set_supportfb(true);
    _brokerVersionInfo.set_supportmergemsg(true);
}

BrokerWorker::~BrokerWorker() {
    _amonitorThreadPtr.reset();
    _checkMonitorThreadPtr.reset();
    _statusReportThreadPtr.reset();
    DELETE_AND_SET_NULL(_taskInfoMonitor);
    DELETE_AND_SET_NULL(_transporterImpl);
    DELETE_AND_SET_NULL(_statusUpdater);
    DELETE_AND_SET_NULL(_tpSupervisor);
    DELETE_AND_SET_NULL(_ipMapper);
    DELETE_AND_SET_NULL(_brokerConfig);
    DELETE_AND_SET_NULL(_metricsReporter);
    DELETE_AND_SET_NULL(_cpu);
    DELETE_AND_SET_NULL(_mem);
    _adminAdapter.reset();
    _channelManager.reset();
}

void BrokerWorker::doInitOptions() {
    getOptionParser().addOption("-c", "--config", CONFIG, OptionParser::OPT_STRING, true);
    getOptionParser().addOption("-r", "--role", ROLE, OptionParser::OPT_STRING, true);
}

void BrokerWorker::doExtractOptions() {
    getOptionParser().getOptionValue(CONFIG, _configPath);
    getOptionParser().getOptionValue(ROLE, _roleName);
}

bool BrokerWorker::initMonitor() {
    string slaveIp = autil::EnvUtil::getEnv("HIPPO_SLAVE_IP", "127.0.0.1");
    string hippoName = autil::EnvUtil::getEnv("HIPPO_SERVICE_NAME", "unknown");
    string instance = autil::EnvUtil::getEnv("HIPPO_APP", "unknown");
    string port = autil::EnvUtil::getEnv("kmonitorPort", "4141");
    string serviceName = autil::EnvUtil::getEnv("kmonitorServiceName", instance);
    string sinkAddress = autil::EnvUtil::getEnv("kmonitorSinkAddress", slaveIp);
    string tenant = autil::EnvUtil::getEnv("kmonitorTenant", "swift");
    string host;
    ::kmonitor::Util::GetHostIP(host);
    MetricsConfig metricsConfig;
    metricsConfig.set_tenant_name(tenant);
    metricsConfig.set_service_name(serviceName);
    metricsConfig.set_sink_address(sinkAddress + ":" + port);
    metricsConfig.set_use_common_tag(false);
    metricsConfig.set_inited(true);
    metricsConfig.AddGlobalTag("instance", instance);
    metricsConfig.AddGlobalTag("host", host);
    metricsConfig.AddGlobalTag("hippo", hippoName);

    bool enableKmonLogFileSink = false, enableKmonPromSink = false;
    enableKmonLogFileSink = autil::EnvUtil::getEnv("kmonitorEnableLogFileSink", enableKmonLogFileSink);
    enableKmonPromSink = autil::EnvUtil::getEnv("kmonitorEnablePrometheusSink", enableKmonPromSink);
    metricsConfig.set_enable_log_file_sink(enableKmonLogFileSink);
    metricsConfig.set_enable_prometheus_sink(enableKmonPromSink);

    string groupName, brokerName;
    if (!PathDefine::parseRoleGroup(_roleName, groupName, brokerName)) {
        fprintf(stderr, "parse role name failed");
        return false;
    }
    metricsConfig.AddGlobalTag("group", groupName);
    metricsConfig.AddGlobalTag("broker", brokerName);

    cout << "init kmonitor, tenant[" << tenant << "], instance[" << instance << "], host[" << host << "]" << endl;
    fprintf(stdout, "init kmonitor factory with[%s]", ToJsonString(metricsConfig, true).c_str());
    AUTIL_LOG(INFO, "init kmonitor factory with[%s]", ToJsonString(metricsConfig, true).c_str());
    _initMonitorLog = ToJsonString(metricsConfig, true);
    if (!KMonitorFactory::Init(metricsConfig)) {
        fprintf(stderr, "init kmonitor factory failed with[%s]", ToJsonString(metricsConfig, true).c_str());
        return false;
    }
    KMonitorFactory::Start();
    cout << "KMonitorFactory::Start() finished" << endl;
    KMonitorFactory::registerBuildInMetrics(nullptr, "swift.worker.broker");
    cout << "KMonitorFactory::registerBuildInMetrics() finished" << endl;
    AUTIL_LOG(INFO, "KMonitorFactory::registerBuildInMetrics() finished");
    _initMonitorLog += "   KMonitorFactory::registerBuildInMetrics() finished";
    return true;
}

bool BrokerWorker::doInit() {
    AUTIL_LOG(INFO, "initMonitor log[%s]", _initMonitorLog.c_str());
    if (!loadConfig()) {
        return false;
    }
    initFileSystem(_brokerConfig->dfsRoot);
    if (!_brokerConfig->supportMergeMsg) {
        _brokerVersionInfo.set_supportmergemsg(false);
    }
    if (!_brokerConfig->supportFb) {
        _brokerVersionInfo.set_supportfb(false);
    }
    PanguInlineFileUtil::setInlineFileMock(_brokerConfig->localMode);
    _cpu = new autil::metric::Cpu();
    _mem = new autil::metric::ProcessMemory();
    _metricsReporter = new BrokerMetricsReporter(_brokerConfig->getReportMetricThreadNum());
    _tpSupervisor = new TopicPartitionSupervisor(_brokerConfig, &_status, _metricsReporter);
    _transporterImpl = new (nothrow) TransporterImpl(_tpSupervisor, _metricsReporter);
    if (!initIpMapper()) {
        AUTIL_LOG(ERROR, "init ip mapper fail");
    }
    _transporterImpl->setIpMapper(_ipMapper);
    if (!_transporterImpl->init(_brokerConfig)) {
        DELETE_AND_SET_NULL(_transporterImpl);
        AUTIL_LOG(ERROR, "init transporter fail");
        return false;
    }
    if (!registerService(_transporterImpl)) {
        DELETE_AND_SET_NULL(_transporterImpl);
        AUTIL_LOG(ERROR, "broker arpc server register service fail");
        return false;
    }
    if (!initHTTPRPCServer(_brokerConfig->httpThreadNum, _brokerConfig->httpQueueSize)) {
        AUTIL_LOG(WARN, "init http arpc server failed!");
        return false;
    }
    _taskInfoMonitor = new TaskInfoMonitor();
    uint32_t rpcPort = 0, httpPort = 0;
    if (_brokerConfig->useRecommendPort && PathDefine::readRecommendPort(rpcPort, httpPort)) {
        AUTIL_LOG(INFO, "recommend rpc port[%d], http port[%d]", rpcPort, httpPort);
        setPort(rpcPort);
        setHTTPPort(httpPort);
    }
    if (!initAdminClient()) {
        AUTIL_LOG(ERROR, "init admin client fail");
        return false;
    }
    _tpSupervisor->setChannelManager(_channelManager);
    _brokerHealthChecker.reset(new BrokerHealthChecker());
    if (!_brokerHealthChecker->init(_brokerConfig->dfsRoot, _brokerConfig->zkRoot, _roleName)) {
        AUTIL_LOG(ERROR, "init broker health checker fail");
        return false;
    }
    return true;
}

bool BrokerWorker::loadConfig() {
    if (fslib::EC_TRUE != fslib::fs::FileSystem::isDirectory(_configPath)) {
        AUTIL_LOG(ERROR, "Invalid config dir[%s]", _configPath.c_str());
        return false;
    }
    string configFile = fslib::fs::FileSystem::joinFilePath(_configPath, SWIFT_CONF_FILE_NAME);
    BrokerConfigParser configParser;
    _brokerConfig = configParser.parse(configFile);
    if (_brokerConfig == NULL) {
        AUTIL_LOG(WARN, "BrokerWorker init failed : load swift config failed!");
        return false;
    }
    if (!_brokerConfig->validate()) {
        AUTIL_LOG(WARN, "BrokerWorker init failed : swift config validate failed!");
        DELETE_AND_SET_NULL(_brokerConfig);
        return false;
    }
    if (_brokerConfig->randomDelObsoleteFileInterval) {
        uint32_t addSec = autil::HashAlgorithm::hashString(_roleName.c_str(), _roleName.size(), 0) % 120;
        AUTIL_LOG(INFO, "%s add[%ds] for delele file interval", _roleName.c_str(), addSec);
        _brokerConfig->addDelObsoleteFileIntervalSec(addSec);
    }
    const string &configStr = _brokerConfig->getConfigStr();
    AUTIL_LOG(INFO, "%s config[%s]", _roleName.c_str(), configStr.c_str());
    return true;
}

void BrokerWorker::initFileSystem(const std::string &dfsRoot) {
    fslib::FileList files;
    fslib::ErrorCode ec = FileSystem::listDir(dfsRoot, files);
    if (fslib::EC_OK != ec) {
        AUTIL_LOG(WARN,
                  "list dir[%s] failed, fslib error[%d %s]",
                  dfsRoot.c_str(),
                  ec,
                  FileSystem::getErrorString(ec).c_str());
    }
    AUTIL_LOG(INFO, "list dir[%s] success", dfsRoot.c_str());
}

bool BrokerWorker::doStart() {
    string lockDir = PathDefine::getBrokerLockDir(_brokerConfig->zkRoot, _roleName);
    int64_t leaderLeaseTimeMs = _brokerConfig->leaderLeaseTime * 1000;
    int64_t leaderLoopIntervalMs = _brokerConfig->leaderLoopInterval * 1000;
    string accKey = getAccessKey();
    if (!initLeaderElectorNotStart(lockDir, leaderLeaseTimeMs, leaderLoopIntervalMs, accKey)) {
        AUTIL_LOG(WARN, "broker init leader elector failed, zkRoot[%s]", _brokerConfig->zkRoot.c_str());
        return false;
    }
    if (!startLeaderElector()) {
        AUTIL_LOG(WARN, "broker start leader elector failed, zkRoot[%s]", _brokerConfig->zkRoot.c_str());
        return false;
    }
    if (!waitToBeLeader(BrokerWorker::LEADER_RETRY_COUNT, BrokerWorker::LEADER_RETRY_TIME_INTERVAL)) {
        AUTIL_LOG(WARN, "failed to be leader!");
        return false;
    }
    auto zkDataAccessor = std::make_shared<ZkDataAccessor>();
    zkDataAccessor->init(getZkWrapper(), _brokerConfig->zkRoot);
    _tpSupervisor->start(std::move(zkDataAccessor));
    _sessionId = TimeUtility::currentTime();
    string taskInfoPath = PathDefine::getTaskInfoPath(_brokerConfig->zkRoot, _roleName);
    if (!_taskInfoMonitor->init(taskInfoPath, _sessionId)) {
        AUTIL_LOG(ERROR, "Failed to Set Parameter In TaskInfoMonitor [%s]", taskInfoPath.c_str());
        return false;
    }
    _statusUpdater = new PartitionStatusUpdater(_tpSupervisor);
    _taskInfoMonitor->setZkConnectionStatus(&_status);
    _taskInfoMonitor->setStatusUpdater(_statusUpdater);
    if (!initHeartbeat()) {
        return false;
    }
    heartbeat(); // send first empty heartbeat
    if (!_taskInfoMonitor->start()) {
        AUTIL_LOG(ERROR, "Failed to Start TaskInfo Monitor");
        return false;
    }

    _checkMonitorThreadPtr = autil::LoopThread::createLoopThread(
        std::bind(&BrokerWorker::checkMonitor, this), CHECK_MONITOR_INTERVAL, "check_monitor", true);
    _amonitorThreadPtr = autil::LoopThread::createLoopThread(
        std::bind(&BrokerWorker::amonReport, this), AMON_REPORT_INTERVAL, "amon_report");
    _statusReportThreadPtr = autil::LoopThread::createLoopThread(std::bind(&BrokerWorker::reportWorkerInMetric, this),
                                                                 _brokerConfig->statusReportIntervalSec * 1000 * 1000,
                                                                 "status_report");
    PathDefine::writeRecommendPort(getPort(), getHTTPPort());
    AUTIL_LOG(INFO, "broker worker is started, session id[%ld]", _sessionId);
    return true;
}

void BrokerWorker::doStop() {
    AUTIL_LOG(INFO, "broker worker is stopping...");
    _amonitorThreadPtr.reset();
    closeRpcServer();
    if (_transporterImpl) {
        _transporterImpl->stop();
    }

    _checkMonitorThreadPtr.reset();
    _statusReportThreadPtr.reset();
    _brokerHealthChecker.reset();
    _taskInfoMonitor->stop();
    if (_tpSupervisor) {
        _tpSupervisor->stop();
    }
    destoryRpcServer();
    heartbeat(); // send last heartbeat
    AUTIL_LOG(INFO, "stopping heartbeat reporter...");
    _heartbeatReporter.shutdown();
    AUTIL_LOG(INFO, "stopping leader elector...");
    shutdownLeaderElector();
    AUTIL_LOG(INFO, "broker worker is stoped.");
}

void BrokerWorker::doIdle() { heartbeat(); }

void BrokerWorker::becomeLeader() {
    AUTIL_LOG(
        INFO, "become leader arpc address [%s], http address [%s]", getAddress().c_str(), getHTTPAddress().c_str());
}

string BrokerWorker::getAccessKey() {
    string key = getAddress() + "_" + StringUtil::toString(getpid());
    return key;
}

void BrokerWorker::noLongerLeader() {
    ScopedLock lock(_exitMutex); // need lock
    _status.setHeartbeatStatus(false, TimeUtility::currentTime());
    AUTIL_LOG(WARN, "broker is no longer leader!");
    cout << getAddress() << " is no longer leader, exit now" << endl;
    stop();
}

void BrokerWorker::amonReport() {
    if (_tpSupervisor) {
        _tpSupervisor->amonReport();
    }
    BrokerSysMetricsCollector collector;
    arpc::ANetRPCServer *rpcServer = getRPCServer();
    if (rpcServer) {
        collector.brokerQueueSize = rpcServer->GetThreadPool()->getItemCount();
    }
    collector.readThreadNum = _transporterImpl->getReadThreadNum();
    collector.getMsgQueueSize = _transporterImpl->getMessageQueueSize();
    collector.holdQueueSize = _transporterImpl->getHoldQueueSize();
    collector.holdTimeoutCount = _transporterImpl->getHoldTimeoutCount();
    collector.clearPollingQueueSize = _transporterImpl->getClearPollingQueueSize();
    _metricsReporter->reportBrokerSysMetrics(collector);
}

void BrokerWorker::checkMonitor() {
    if (_taskInfoMonitor->isBad() || !_status.isZkMonitorAlive()) {
        _taskInfoMonitor->stop();
        _taskInfoMonitor->start();
    }
}

string BrokerWorker::getWorkerAddress() {
    return PathDefine::getRoleAddress(_roleName, getIp(), (uint16_t)(getPort()));
}

string BrokerWorker::getHeartbeatAddress() {
    return PathDefine::heartbeatReportAddress(_brokerConfig->zkRoot, getWorkerAddress());
}

HeartbeatInfo BrokerWorker::getHeartbeatInfo() {
    HeartbeatInfo info;
    _tpSupervisor->getPartitionStatus(info);
    return info;
}

bool BrokerWorker::initHeartbeat() {
    string address = getHeartbeatAddress();
    if (!_heartbeatReporter.setParameter(address)) {
        AUTIL_LOG(ERROR, "HeartBeatReporter setParameter error. [%s]", address.c_str());
        return false;
    }
    return true;
}

void BrokerWorker::heartbeat() {
    HeartbeatInfo info = getHeartbeatInfo();
    info.set_address(getWorkerAddress());
    info.set_alive(true);
    info.set_sessionid(_sessionId);
    info.set_heartbeatid(TimeUtility::currentTime());
    *(info.mutable_versioninfo()) = _brokerVersionInfo;
    bool leader = isLeader();
    if (leader && _heartbeatReporter.heartBeat(info)) {
        _status.setHeartbeatStatus(true, TimeUtility::currentTime());
    } else {
        AUTIL_LOG(WARN, "send heartbeat error, is leader[%d]", (int)leader);
        _status.setHeartbeatStatus(false, TimeUtility::currentTime());
    }
}

bool BrokerWorker::initAdminClient() {
    _channelManager.reset(new SwiftRpcChannelManager());
    if (!_channelManager->init(NULL)) {
        AUTIL_LOG(ERROR, "init swift rpc channel manager  failed.");
        return false;
    }
    _adminAdapter.reset(new SwiftAdminAdapter(_brokerConfig->zkRoot, _channelManager, false));
    ClientAuthorizerInfo info;
    _brokerConfig->authConf.getInnerSysUserPasswd(info.username, info.passwd);
    _adminAdapter->setAuthenticationConf(info);
    return true;
}

bool BrokerWorker::initIpMapper() {
    string fileName = _brokerConfig->ipMapFile;
    if (fileName.empty()) {
        return true;
    }
    string content;
    if (fileName[0] == '.') {
        string instRoot = autil::EnvUtil::getEnv("HIPPO_APP_INST_ROOT", "");
        if (!instRoot.empty()) {
            fileName = instRoot + "/" + fileName;
        }
    }
    AUTIL_LOG(INFO, "read ipmap file [%s]", fileName.c_str());

    auto ec = fslib::fs::FileSystem::readFile(fileName, content);
    if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "read file [%s] failed", _brokerConfig->ipMapFile.c_str());
        return false;
    }
    map<string, string> strMap;
    try {
        autil::legacy::FromJsonString(strMap, content);
    } catch (...) {
        AUTIL_LOG(ERROR, "parse content failed: %s", content.c_str());
        return false;
    }
    unordered_map<string, string> ipMap(strMap.begin(), strMap.end());
    AUTIL_LOG(INFO, "ipmap size [%lu]", ipMap.size());
    _ipMapper = new IpMapper(ipMap);
    return true;
}

void BrokerWorker::reportWorkerInMetric() {
    if (NULL == _tpSupervisor || nullptr == _adminAdapter || nullptr == _brokerHealthChecker) {
        return;
    }
    int64_t curTime = TimeUtility::currentTime();
    if (!_brokerHealthChecker->checkAllStatus(StringUtil::toString(curTime))) {
        AUTIL_LOG(ERROR, "check status failed");
        return;
    }
    BrokerStatusRequest request;
    BrokerInMetric *metric = request.mutable_status();
    metric->set_reporttime(curTime / 1000000);
    metric->set_rolename(getWorkerAddress());
    _cpu->update();
    metric->set_cpuratio(_cpu->getUser() + _cpu->getSys());
    _mem->update();
    metric->set_memratio(_mem->getMemUsedRatio());
    int64_t zfstimeout = -1;
    _tpSupervisor->getPartitionInMetrics(
        _brokerConfig->statusReportIntervalSec, metric->mutable_partinmetrics(), zfstimeout);
    metric->set_zfstimeout(zfstimeout);
    BrokerStatusResponse response;
    AUTIL_LOG(INFO,
              "report: cpu[%lf], mem[%lf], partCount[%d], zfsTimeout[%ld]",
              metric->cpuratio(),
              metric->memratio(),
              metric->partinmetrics_size(),
              metric->zfstimeout());
    ErrorCode ec = _adminAdapter->reportBrokerStatus(request, response);

    if (ERROR_NONE != ec) {
        AUTIL_LOG(WARN, "report status fail, error:%d, response[%s]", ec, response.ShortDebugString().c_str());
        if (ERROR_ADMIN_NOT_LEADER == ec) {
            _adminAdapter.reset(new SwiftAdminAdapter(_brokerConfig->zkRoot, _channelManager, false));
            ClientAuthorizerInfo info;
            _brokerConfig->authConf.getInnerSysUserPasswd(info.username, info.passwd);
            _adminAdapter->setAuthenticationConf(info);
        }
    }
    if (_metricsReporter) {
        monitor::BrokerInStatusCollector status;
        convertBrokerInMetric2Status(*metric, status);
        _metricsReporter->reportBrokerInStatus(_roleName, status);
    }
}

void BrokerWorker::convertBrokerInMetric2Status(const BrokerInMetric &metric, BrokerInStatusCollector &collector) {
    collector.cpu = metric.cpuratio();
    collector.mem = metric.memratio();
    collector.zfsTimeout = metric.zfstimeout();
    for (int index = 0; index < metric.partinmetrics_size(); ++index) {
        const PartitionInMetric &pMetric = metric.partinmetrics(index);
        collector.writeRate1min += pMetric.writerate1min();
        collector.writeRate5min += pMetric.writerate5min();
        collector.readRate1min += pMetric.readrate1min();
        collector.readRate5min += pMetric.readrate5min();
        collector.writeRequest1min += pMetric.writerequest1min();
        collector.writeRequest5min += pMetric.writerequest5min();
        collector.readRequest1min += pMetric.readrequest1min();
        collector.readRequest5min += pMetric.readrequest5min();
        collector.commitDelay = max(pMetric.commitdelay(), collector.commitDelay);
    }
}

} // namespace service
} // namespace swift
