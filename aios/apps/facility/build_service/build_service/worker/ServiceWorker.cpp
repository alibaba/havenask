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
#include "build_service/worker/ServiceWorker.h"

#include "autil/EnvUtil.h"
#include "beeper/beeper.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/CounterSynchronizer.h"
#include "build_service/common/PathDefine.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/util/BsMetricTagsHandler.h"
#include "build_service/util/KmonitorUtil.h"
#include "build_service/util/LeaderCheckerGuard.h"
#include "build_service/worker/AgentServiceImpl.h"
#include "build_service/worker/BuildJobImpl.h"
#include "build_service/worker/BuilderServiceImpl.h"
#include "build_service/worker/MergerServiceImpl.h"
#include "build_service/worker/ProcessorServiceImpl.h"
#include "build_service/worker/RpcWorkerHeartbeat.h"
#include "build_service/worker/TaskStateHandler.h"
#include "build_service/worker/ZkWorkerHeartbeat.h"
#include "fslib/cache/FSCacheModule.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/util/FileUtil.h"
#include "fslib/util/MetricTagsHandler.h"
#include "indexlib/misc/doc_tracer.h"
#include "indexlib/util/EpochIdUtil.h"
#include "kmonitor/client/KMonitorFactory.h"
#include "kmonitor_adapter/MonitorFactory.h"

using namespace std;
using namespace autil;
using namespace fslib::fs;
using namespace fslib::util;
using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::common;
using namespace build_service::util;
using namespace kmonitor_adapter;
using namespace kmonitor;

using namespace indexlib::index_base;

namespace worker_framework {
WorkerBase* createWorker() { return new build_service::worker::ServiceWorker(); }
}; // namespace worker_framework

using namespace worker_framework;

namespace build_service { namespace worker {
BS_LOG_SETUP(worker, ServiceWorker);

const string worker::ServiceWorker::AMONITOR_AGENT_PORT = "amonitor_port";
const string ServiceWorker::AMONITOR_SERVICE_NAME = "amonitor_servece_name";
const string ServiceWorker::ADMIN_SERVICE_NAME = "admin_service_name";
const string ServiceWorker::HEARTBEAT_TYPE = "heartbeat_type";
const string ServiceWorker::RPC_HEARTBEAT = "rpc";
const string ServiceWorker::LEADER_LEASE_TIMEOUT = "leader_lease_timeout";
const string ServiceWorker::LEADER_SYNC_INTERVAL = "leader_sync_interval";
const string ServiceWorker::BEEPER_CONFIG_PATH = "beeper_config_path";
const string ServiceWorker::HIPPO_ENV_APP_DECLARE_TIMESTAMP = "HIPPO_APP_DECLARE_TIME";

ServiceWorker::ServiceWorker()
    : _hippoDeclareTime(0)
    , _monitor(NULL)
    , _rpcHeartbeat(
          new RpcWorkerHeartbeat(WorkerNodeBase::getIdentifier(getIp(), getSlotId()), _partitionId, NULL, this))
// this pid is empty, remove this pid
{
    _hippoDeclareTime = EnvUtil::getEnv(HIPPO_ENV_APP_DECLARE_TIMESTAMP, (int64_t)0);
}

ServiceWorker::~ServiceWorker() { doStop(); }

void ServiceWorker::doInitOptions()
{
    getOptionParser().addOption("-z", "--zookeeper", config::ZOOKEEPER_ROOT, OptionParser::OPT_STRING, true);
    getOptionParser().addOption("-r", "--role", config::ROLE, OptionParser::OPT_STRING, true);
    getOptionParser().addOption("-s", "--service_name", config::SERVICE_NAME, OptionParser::OPT_STRING, true);
    getOptionParser().addOption("-m", "--admin_service_name", ADMIN_SERVICE_NAME, OptionParser::OPT_STRING, true);
    getOptionParser().addOption("", "--amonitor_service_name", AMONITOR_SERVICE_NAME, OptionParser::OPT_STRING, false);
    getOptionParser().addOption("-a", "--amonitor_port", AMONITOR_AGENT_PORT, OptionParser::OPT_UINT32, true);
    getOptionParser().addOption("-d", "--heartbeat_type", HEARTBEAT_TYPE, OptionParser::OPT_STRING, false);
    getOptionParser().addOption("", "--leader_lease_timeout", LEADER_LEASE_TIMEOUT, OptionParser::OPT_UINT32, false);
    getOptionParser().addOption("", "--leader_sync_interval", LEADER_SYNC_INTERVAL, OptionParser::OPT_UINT32, false);
    getOptionParser().addOption("", "--beeper_config_path", BEEPER_CONFIG_PATH, OptionParser::OPT_STRING, false);
}

template <typename T>
void ServiceWorker::fillTargetArgument(const std::string& optName, const std::string& key,
                                       std::map<std::string, std::string>& argMap)
{
    T value;
    if (!getOptionParser().getOptionValue(optName, value)) {
        return;
    }
    argMap[key] = autil::StringUtil::toString(value);
}

void ServiceWorker::fillProcArgs(std::map<std::string, std::string>& argMap)
{
    fillTargetArgument<std::string>(config::ZOOKEEPER_ROOT, "-z", argMap);
    fillTargetArgument<std::string>(config::ROLE, "-r", argMap);
    fillTargetArgument<std::string>(config::SERVICE_NAME, "-s", argMap);
    fillTargetArgument<std::string>(ADMIN_SERVICE_NAME, "-m", argMap);
    fillTargetArgument<std::string>(AMONITOR_SERVICE_NAME, "--amonitor_service_name", argMap);
    fillTargetArgument<uint32_t>(AMONITOR_AGENT_PORT, "-a", argMap);
    fillTargetArgument<std::string>(HEARTBEAT_TYPE, "-d", argMap);
    fillTargetArgument<uint32_t>(LEADER_LEASE_TIMEOUT, "--leader_lease_timeout", argMap);
    fillTargetArgument<uint32_t>(LEADER_SYNC_INTERVAL, "--leader_sync_interval", argMap);
    fillTargetArgument<std::string>(BEEPER_CONFIG_PATH, "--beeper_config_path", argMap);

    // below defined in worker_framework::WorkerBase
    fillTargetArgument<std::string>("logConf", "-l", argMap);
}

bool ServiceWorker::doInit()
{
    if (!_rpcHeartbeat->start()) {
        return false;
    }
    return true;
}

bool ServiceWorker::doStart()
{
    initBeeper();
    FileSystem::getCacheModule()->reset();
    fillAddr();

    string appName;
    getOptionParser().getOptionValue(config::SERVICE_NAME, appName);

    string roleId;
    getOptionParser().getOptionValue(config::ROLE, roleId);

    string adminServiceName;
    if (!getOptionParser().getOptionValue(ADMIN_SERVICE_NAME, adminServiceName)) {
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, "worker start fail, missing admin_service_name");
        BS_LOG(ERROR, "missing parameter [%s]", ADMIN_SERVICE_NAME.c_str());
        return false;
    }

    string msg = roleId + " worker starting ...";
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
    BS_LOG(INFO, "%s worker starting ...", roleId.c_str());
    _partitionId.Clear();
    if (!extractPartitionId(roleId, adminServiceName, appName, _partitionId)) {
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, "worker start fail, parse partitionId fail");
        BS_LOG(ERROR, "create PartitionId from [%s] failed", roleId.c_str());
        return false;
    }

    string appZkRoot;
    if (!getOptionParser().getOptionValue(config::ZOOKEEPER_ROOT, appZkRoot)) {
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, "worker start fail, missing zk root");
        BS_LOG(ERROR, "missing parameter [%s]", config::ZOOKEEPER_ROOT.c_str());
        return false;
    }

    bool ignoreBackupId = _partitionId.role() == ROLE_PROCESSOR ? true : false;
    string workerZkRoot = PathDefine::getPartitionZkRoot(appZkRoot, _partitionId, ignoreBackupId);

    unique_ptr<LeaderCheckerGuard> lc(new LeaderCheckerGuard(getLeaderElector()));
    uint32_t tmpValue;
    uint32_t leaderLeaseTimeout = 60;
    if (getOptionParser().getOptionValue(LEADER_LEASE_TIMEOUT, tmpValue)) {
        leaderLeaseTimeout = tmpValue;
    }
    uint32_t leaderSyncInterval = 2;
    if (getOptionParser().getOptionValue(LEADER_SYNC_INTERVAL, tmpValue)) {
        leaderSyncInterval = tmpValue;
    }

    if (leaderSyncInterval >= leaderLeaseTimeout / 2) {
        BS_LOG(ERROR, "leaderSyncInterval[%u] is greater than leaderLeaseTimeout / 2 [%u]", leaderSyncInterval,
               leaderLeaseTimeout / 2);
        return false;
    }

    if (!initLeaderElector(workerZkRoot, leaderLeaseTimeout, leaderSyncInterval, "")) {
        BS_LOG(WARN, "initLeaderElector with zkRoot[%s], leaderLeaseTimeout[%u], leaderSyncInterval[%u] failed",
               workerZkRoot.c_str(), leaderLeaseTimeout, leaderSyncInterval);
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, "worker start fail, init leader elector fail");
        return false;
    }

    if (!waitToBeLeader(90)) {
        BS_LOG(WARN, "failed to be leader!");
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, "worker start fail, fail to be leader");
        return false;
    }

    LeaderElector::LeaderState leaderState = getLeaderState();
    if (unlikely(!leaderState.isLeader)) {
        BS_LOG(WARN, "no longer leader!");
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, "worker start fail, no longer leader");
        return false;
    }

    std::string epochId = indexlib::util::EpochIdUtil::GenerateEpochId(leaderState.leaseExpirationTime);
    if (!startMonitor(_partitionId)) {
        BS_LOG(WARN, "start monitor failed");
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, "worker start fail, start monitor fail");
        return false;
    }

    _workerHeartbeatExecutor.reset(createServiceImpl(_partitionId, workerZkRoot, appZkRoot, adminServiceName, epochId));
    if (!_workerHeartbeatExecutor || !_workerHeartbeatExecutor->start()) {
        BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, "worker start fail, start HeartBeatExecutor fail");
        return false;
    }

    _workerHeartbeatExecutor->setExitCallback([this] { releaseLeader(); });
    lc->done();
    BS_LOG(INFO, "%s worker started", roleId.c_str());

    msg = roleId + " worker started";
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);
    return true;
}

void ServiceWorker::doStop()
{
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, "worker stopping");
    stopRPCServer();
    _workerHeartbeatExecutor.reset();
    _rpcHeartbeat.reset();
    releaseLeader();
    BEEPER_CLOSE();
    _metricProvider.reset();
    _reporter.reset();
    PeriodDocCounterHelper::shutdown();
}

bool ServiceWorker::startMonitor(const PartitionId& pid)
{
    string kmonServiceName = EnvUtil::getEnv(BS_ENV_KMON_SERVICE_NAME, BS_ENV_DEFAULT_KMON_SERVICE_NAME);
    BS_LOG(INFO, "set kmonservice name[%s]", kmonServiceName.c_str());
    // TODO, remove this var
    _monitor = kmonitor_adapter::MonitorFactory::getInstance()->createMonitor(kmonServiceName);
    kmonitor::MetricsTags tags;
    KmonitorUtil::getTags(pid, tags);
    BS_LOG(INFO, "set metrics tag[%s]", tags.ToString().c_str());

    string serviceName;
    if (!getOptionParser().getOptionValue(AMONITOR_SERVICE_NAME, serviceName)) {
        getOptionParser().getOptionValue(config::SERVICE_NAME, serviceName);
    }

    _reporter.reset(new kmonitor::MetricsReporter(kmonServiceName, "", tags));
    _metricProvider.reset(new indexlib::util::MetricProvider(_reporter));

    MetricTagsHandlerPtr tagsHandler(new BsMetricTagsHandler(_partitionId));
    FileSystem::setMetricTagsHandler(tagsHandler);

    if (_hippoDeclareTime > 0) {
        _hippoStartProcInterval =
            DECLARE_METRIC(_metricProvider, "service/hippoStartProcInterval", kmonitor::STATUS, "us");
        REPORT_METRIC(_hippoStartProcInterval, _addr.starttimestamp() - _hippoDeclareTime);
    }
    _waitToBeLeaderInterval = DECLARE_METRIC(_metricProvider, "service/waitToBeLeaderInterval", kmonitor::STATUS, "us");
    REPORT_METRIC(_waitToBeLeaderInterval, autil::TimeUtility::currentTime() - _addr.starttimestamp());
    return true;
}

WorkerHeartbeatExecutor* ServiceWorker::createServiceImpl(const PartitionId& pid, const string& workerZkRoot,
                                                          const string& appZkRoot, const string& adminServiceName,
                                                          const std::string& epochId)
{
    WorkerNodeBase* workerNode;
    WorkerStateHandler* handler;
    WorkerHeartbeat* heartbeat;
    string roleName;
    ProtoUtil::partitionIdToRoleId(pid, roleName);
    stringstream ss;
    ss << pid.buildid().generationid() << " " << roleName << " " << getIp();
    DECLARE_ERROR_COLLECTOR_IDENTIFIER(ss.str());
    string counterZkRoot = CounterSynchronizer::getCounterZkRoot(appZkRoot, pid.buildid());
    if (pid.role() == ROLE_BUILDER) {
        workerNode = new BuilderNode(pid);
        handler = new BuilderServiceImpl(pid, _metricProvider, _addr, appZkRoot, adminServiceName, epochId);
    } else if (pid.role() == ROLE_PROCESSOR) {
        workerNode = new ProcessorNode(pid);
        handler = new ProcessorServiceImpl(pid, _metricProvider, _addr, appZkRoot, adminServiceName);
    } else if (pid.role() == ROLE_MERGER) {
        workerNode = new MergerNode(pid);
        handler =
            new MergerServiceImpl(pid, _metricProvider, _addr, workerZkRoot, appZkRoot, adminServiceName, epochId);
    } else if (pid.role() == ROLE_JOB) {
        workerNode = new JobNode(pid);
        handler = new BuildJobImpl(pid, _metricProvider, _addr, counterZkRoot);
    } else if (pid.role() == ROLE_TASK) {
        workerNode = new TaskNode(pid);
        handler = new TaskStateHandler(pid, _metricProvider, _addr, appZkRoot, adminServiceName, epochId);
    } else if (pid.role() == ROLE_AGENT) {
        workerNode = new AgentNode(pid);
        std::map<std::string, std::string> args;
        fillProcArgs(args);
        handler = new AgentServiceImpl(args, pid, _metricProvider, _addr, appZkRoot, adminServiceName);
    } else {
        return NULL;
    }
    assert(handler != NULL);
    assert(workerNode != NULL);

    if (!handler->init()) {
        BS_LOG(ERROR, "init handler [%s] failed", pid.ShortDebugString().c_str());
        return NULL;
    }

    string heartbeatType;
    getOptionParser().getOptionValue(HEARTBEAT_TYPE, heartbeatType);
    if (heartbeatType == RPC_HEARTBEAT) {
        // real pid
        _rpcHeartbeat->setPid(pid);
        _rpcHeartbeat->setWorkerNode(workerNode);
        _rpcHeartbeat->setStarted(true);
        heartbeat = _rpcHeartbeat.release();
        BS_LOG(INFO, "worker rpc heartbeat enabled");
    } else {
        heartbeat = new ZkWorkerHeartbeat(workerNode, workerZkRoot, getZkWrapper());
        BS_LOG(INFO, "worker zookeeper heartbeat enabled");
    }
    assert(heartbeat);
    return new WorkerHeartbeatExecutor(workerNode, handler, heartbeat);
}

void ServiceWorker::doIdle()
{
    if (!isLeader()) {
        exit("no longer leader, exit now");
    }
}

void ServiceWorker::noLongerLeader() { exit("no longer leader, exit now"); }

void ServiceWorker::exit(const std::string& message)
{
    const string& address = getAddress();
    BS_LOG(ERROR, "%s %s", address.c_str(), message.c_str());
    cerr << address << " " << message << endl;
    string msg = address + " " + message;
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);

    releaseLeader();
    BS_LOG_FLUSH();
    BEEPER_CLOSE();
    BS_LOG(ERROR, "exit");
    _exit(-1);
}

void ServiceWorker::releaseLeader()
{
    // fix aone xxxx://invalid/issue/48916071
    bool expect = false;
    if (_alreadyReleasedLeader.compare_exchange_strong(expect, true)) {
        shutdownLeaderElector();
    }
}

void ServiceWorker::fillAddr()
{
    _addr.set_ip(getIp());
    _addr.set_arpcport(getPort());
    _addr.set_httpport(getHTTPPort());
    _addr.set_pid(getpid());
    _addr.set_starttimestamp(autil::TimeUtility::currentTime());
}

void ServiceWorker::initBeeper()
{
    string beeperConfigPath;
    getOptionParser().getOptionValue(BEEPER_CONFIG_PATH, beeperConfigPath);
    if (beeperConfigPath.empty()) {
        char buffer[1024];
        int ret = readlink("/proc/self/exe", buffer, sizeof(buffer) - 1);
        // .../usr/local/bin
        string binaryRoot = fslib::util::FileUtil::getParentDir(string(buffer, ret));
        // .../usr/local/etc/bs/bs_beeper.json
        beeperConfigPath = fslib::util::FileUtil::joinFilePath(fslib::util::FileUtil::getParentDir(binaryRoot),
                                                               "etc/bs/bs_beeper.json");
        BS_LOG(INFO, "not set beeper_config_path, use local path beeper config [%s]", beeperConfigPath.c_str());
    } else {
        BS_LOG(INFO, "use beeper_config_path parameter [%s]", beeperConfigPath.c_str());
    }

    string content;
    if (!fslib::util::FileUtil::readFile(beeperConfigPath, content)) {
        BS_LOG(WARN, "init beeper error, read beeper config file [%s] fail!", beeperConfigPath.c_str());
        return;
    }

    beeper::EventTags globalTags;
    string appName;
    getOptionParser().getOptionValue(config::SERVICE_NAME, appName);
    globalTags.AddTag("appName", appName);

    string roleId;
    getOptionParser().getOptionValue(config::ROLE, roleId);
    globalTags.AddTag("roleId", roleId);

    string adminServiceName;
    getOptionParser().getOptionValue(ADMIN_SERVICE_NAME, adminServiceName);
    globalTags.AddTag("adminServiceName", adminServiceName);

    PartitionId pid;
    if (extractPartitionId(roleId, adminServiceName, appName, pid)) {
        ProtoUtil::partitionIdToBeeperTags(pid, globalTags);
    }
    BEEPER_INIT_FROM_CONF_STRING(content, globalTags);
    DECLARE_BEEPER_COLLECTOR(WORKER_STATUS_COLLECTOR_NAME);
    DECLARE_BEEPER_COLLECTOR(WORKER_ERROR_COLLECTOR_NAME);
    DECLARE_BEEPER_COLLECTOR(IE_DOC_TRACER_COLLECTOR_NAME);

    DECLARE_BEEPER_COLLECTOR(FSLIB_REMOVE_COLLECTOR_NAME);
    DECLARE_BEEPER_COLLECTOR(FSLIB_ERROR_CODE_NOT_SUPPORT);
    DECLARE_BEEPER_COLLECTOR(FSLIB_LONG_INTERVAL_COLLECTOR_NAME);

    DECLARE_BEEPER_COLLECTOR(INDEXLIB_BUILD_INFO_COLLECTOR_NAME);
    DECLARE_BEEPER_COLLECTOR(WORKER_PROCESS_ERROR_COLLECTOR_NAME);
}

bool ServiceWorker::extractPartitionId(const std::string& roleName, const std::string& adminServiceName,
                                       const std::string& appName, proto::PartitionId& pid) const
{
    if (ProtoUtil::roleIdToPartitionId(roleName, appName, pid)) {
        return true;
    }
    pid.Clear();
    if (!ProtoUtil::roleIdToPartitionId(roleName, adminServiceName, pid)) {
        return false;
    }
    // agentNode use adminServiceName as the first part of roleId
    return pid.role() == ROLE_AGENT;
}

bool ServiceWorker::needSlotIdInLeaderInfo() const { return true; }

}} // namespace build_service::worker
