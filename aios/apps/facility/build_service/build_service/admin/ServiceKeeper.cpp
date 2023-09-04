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
#include "build_service/admin/ServiceKeeper.h"

#include <google/protobuf/util/json_util.h>

#include "autil/HashAlgorithm.h"
#include "autil/LoopThread.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "build_service/admin/AgentSimpleMasterScheduler.h"
#include "build_service/admin/ConfigValidator.h"
#include "build_service/admin/GenerationRecoverWorkItem.h"
#include "build_service/admin/taskcontroller/GeneralTaskController.h"
#include "build_service/common/CounterSynchronizer.h"
#include "build_service/common/PathDefine.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/RoleNameGenerator.h"
#include "build_service/util/Monitor.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"

using namespace std;
using namespace autil;

USE_MASTER_FRAMEWORK_NAMESPACE(simple_master);
using namespace worker_framework;

using namespace build_service::proto;
using namespace build_service::common;
using namespace build_service::util;
using namespace build_service::config;
using namespace build_service::admin;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ServiceKeeper);

ServiceKeeper::ServiceKeeperMetric::ServiceKeeperMetric() {}

void ServiceKeeper::ServiceKeeperMetric::init(kmonitor_adapter::Monitor* monitor)
{
    if (!monitor) {
        return;
    }
    krpcRequestLatency = monitor->registerGaugePercentileMetric("rpc.rpcRequestLatency", kmonitor::FATAL);
    krpcRequestQps = monitor->registerQPSMetric("rpc.requestProcessQps", kmonitor::FATAL);
    kstartBuildLatency = monitor->registerGaugePercentileMetric("rpc.startBuildLatency", kmonitor::FATAL);
    kstartBuildQps = monitor->registerQPSMetric("rpc.startBuildQps", kmonitor::FATAL);
    kstartTaskLatency = monitor->registerGaugePercentileMetric("rpc.startTaskLatency", kmonitor::FATAL);
    kstartTaskQps = monitor->registerQPSMetric("rpc.startTaskQps", kmonitor::FATAL);
    kgsLatency = monitor->registerGaugePercentileMetric("rpc.getServiceInfoLatency", kmonitor::FATAL);
    kgsQps = monitor->registerQPSMetric("rpc.getServiceInfoQps", kmonitor::FATAL);
    kstopBuildQps = monitor->registerQPSMetric("rpc.stopBuildQps", kmonitor::FATAL);
    kstopTaskQps = monitor->registerQPSMetric("rpc.stopTaskQps", kmonitor::FATAL);
}

ServiceKeeper::ServiceKeeper()
    : _zkWrapper(NULL)
    , _zkStatus(NULL)
    , _scheduler(NULL)
    , _cleaner(NULL)
    , _monitor(NULL)
    , _refreshTimestamp(0)
    , _scheduleTimestamp(0)
    , _lastSyncScheduleTs(0)
{
    _kmonServiceName = autil::EnvUtil::getEnv(BS_ENV_ADMIN_KMON_SERVICE_NAME, BS_ENV_ADMIN_DEFAULT_KMON_SERVICE_NAME);
}

ServiceKeeper::~ServiceKeeper()
{
    stop();
    DELETE_AND_SET_NULL(_scheduler);
    DELETE_AND_SET_NULL(_cleaner);
    DELETE_AND_SET_NULL(_zkStatus);
}

bool ServiceKeeper::start(const string& zkRoot, const string& adminServiceName, cm_basic::ZkWrapper* zkWrapper,
                          SimpleMasterScheduler* scheduler, uint16_t amonitorPort, uint32_t prohibitTime,
                          uint32_t prohibitNum, kmonitor_adapter::Monitor* monitor)
{
    if (prohibitNum != 0) {
        _prohibitedIpsTable.reset(new ProhibitedIpsTable((int64_t)prohibitTime * 1000 * 1000, (size_t)prohibitNum));
    }
    _zkRoot = zkRoot;
    _adminServiceName = adminServiceName;
    _zkWrapper = zkWrapper;
    _scheduler = scheduler;
    _amonitorPort = amonitorPort;
    _monitor = monitor;
    _zkStatus = new ZkState(zkWrapper, PathDefine::getAdminScheduleTimestampFilePath(zkRoot));
    if (_monitor) {
        _metricsReporter.reset(new AdminMetricsReporter);
        _metricsReporter->init(monitor);
        _keeperMetric.init(monitor);
    }

    // read from pkgList if specified in env
    _specifiedWorkerPkgList = prepareSpecifiedWorkerPkgList();

    vector<BuildId> buildIds;
    if (!collectRecoverBuildIds(buildIds)) {
        string errorMsg = "collect all recover generation info from zk failed!";
        BEEPER_REPORT(ADMIN_STATUS_COLLECTOR_NAME, errorMsg);
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    _refreshTimestamp = TimeUtility::currentTimeInSeconds();
    _scheduleTimestamp = _refreshTimestamp;
    string lastScheduleTsStr;
    if (_zkStatus && _zkStatus->read(lastScheduleTsStr) == WorkerState::EC_OK) {
        int64_t timestamp = _scheduleTimestamp;
        StringUtil::fromString(lastScheduleTsStr, timestamp);
        _scheduleTimestamp = timestamp;
    }

    _healthChecker.reset(new AdminHealthChecker(_monitor));
    if (!_healthChecker->init(this)) {
        BEEPER_REPORT(ADMIN_STATUS_COLLECTOR_NAME, "init Admin health checker fail!");
        return false;
    }
    _memController.reset(new TCMallocMemController());
    if (!_memController->start()) {
        BEEPER_REPORT(ADMIN_STATUS_COLLECTOR_NAME, "start Admin memController fail!");
        return false;
    }

    if (!_serviceInfoFilter.init(_zkWrapper, _zkRoot)) {
        string errorMsg = "AdminWorker init failed : ServiceInfoFilter init failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    string amonPortStr = StringUtil::toString(_amonitorPort);

    auto agentMaintainer =
        std::make_shared<GlobalAgentMaintainer>(_zkWrapper, _zkRoot, _adminServiceName, amonPortStr, _monitor);
    if (!agentMaintainer->initFromZkRoot()) {
        string errorMsg = "AdminWorker init failed : GlobalAgentMaintainer init failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    setGlobalAgentMaintainer(agentMaintainer);

    if (!recover(buildIds)) {
        string errorMsg = "Some Generation Recover failed!";
        BEEPER_REPORT(ADMIN_STATUS_COLLECTOR_NAME, errorMsg);
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        // todo
        _recoverThread = Thread::createThread(bind(&ServiceKeeper::recoverLoop, this), "BsAdminRecover");
        if (!_recoverThread) {
            return false;
        }
    }

    _cleaner = new ServiceKeeperObsoleteDataCleaner(this);
    if (!_cleaner->start()) {
        string errorMsg = "AdminWorker init failed : start cleaner failed!";
        BEEPER_REPORT(ADMIN_STATUS_COLLECTOR_NAME, errorMsg);
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }

    _indexInfoCollector.reset(new IndexInfoCollector(_zkWrapper, _zkRoot));
    if (!_indexInfoCollector->init(monitor)) {
        BEEPER_REPORT(ADMIN_STATUS_COLLECTOR_NAME, "init index info collector fail");
        return false;
    }

    if (CounterSynchronizer::needSyncCounter()) {
        BS_LOG(INFO, "support get counter infos");
    }

    _counterSyncThread =
        LoopThread::createLoopThread(bind(&ServiceKeeper::syncAdminInfo, this), getSyncCounterInterval() * 1000 * 1000);
    if (!_counterSyncThread) {
        return false;
    }

    _loopThread = LoopThread::createLoopThread(bind(&ServiceKeeper::keepServiceLoop, this), 1000 * 1000);
    if (!_loopThread) {
        return false;
    }

    if (!doInit()) {
        BS_LOG(ERROR, "do init failed");
        return false;
    }
    if (!_scheduler->start()) {
        string errorMsg = "AdminWorker init failed : start scheduler failed!";
        BEEPER_REPORT(ADMIN_STATUS_COLLECTOR_NAME, errorMsg);
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }

    return true;
}

void ServiceKeeper::stop()
{
    if (_counterSyncThread) {
        _counterSyncThread->stop();
    }
    if (_loopThread) {
        _loopThread->stop();
    }
    {
        autil::ScopedLock lock(_mapLock);
        _activeGenerationKeepers.clear();
        _allGenerationKeepers.clear();
        _recoverFailedGenerations.clear();
    }
    _recoverThread.reset();
};

std::vector<hippo::PackageInfo> ServiceKeeper::prepareSpecifiedWorkerPkgList()
{
    std::vector<hippo::PackageInfo> pkgList;
    std::string specifiedPackageFile = "";
    if ((EnvUtil::getEnvWithoutDefault("specified_worker_package_file", specifiedPackageFile))) {
        string fileContent;
        if (!fslib::util::FileUtil::readFile(specifiedPackageFile, fileContent)) {
            string errorMsg = "read specifiedPackageFile failed: file [" + specifiedPackageFile + "]";
            BS_LOG(WARN, "%s", errorMsg.c_str());
            return {};
        }
        try {
            FromJsonString(pkgList, fileContent);
        } catch (const autil::legacy::ExceptionBase& e) {
            stringstream ss;
            ss << "Invalid json file[" << specifiedPackageFile << "], content[" << fileContent << "], exception["
               << string(e.what()) << "], do not specified";
            string errorMsg = ss.str();
            BS_LOG(WARN, "%s", errorMsg.c_str());
            return {};
        }
        if (!pkgList.empty()) {
            BS_LOG(INFO, "specified package info to [%s]", fileContent.c_str());
        }
    }
    return pkgList;
}

void ServiceKeeper::fillTaskInfo(const proto::TaskInfoRequest* request, proto::TaskInfoResponse* response)
{
    const BuildId& buildId = request->buildid();
    bool isRecoverFailed = false;
    GenerationKeeperPtr generationKeeper = getGeneration(buildId, true, isRecoverFailed, /*fuzzy*/ true);
    if (!generationKeeper) {
        response->set_taskexist(false);
        BS_LOG(ERROR, "get task info fail: build id [%s] not exist", buildId.ShortDebugString().c_str());
        return;
    }

    if (isRecoverFailed) {
        response->set_taskexist(false);
        BS_LOG(ERROR, "get task info fail: build id [%s] recover failed", buildId.ShortDebugString().c_str());
        return;
    }
    int64_t taskId = request->taskid();
    if (taskId < 0) {
        response->set_taskexist(false);
        BS_LOG(ERROR, "get task info fail: invalid taskid [%ld]", taskId);
        return;
    }
    bool isExist = false;
    generationKeeper->fillTaskInfo(taskId, response->mutable_taskinfo(), isExist);
    response->set_taskexist(isExist);
    return;
}

void ServiceKeeper::fillWorkerRoleInfoMap(
    const std::vector<GenerationKeeperPtr>& keepers, bool onlyActiveRole,
    std::map<proto::BuildId, GenerationKeeper::WorkerRoleInfoMapPtr>& multiGenerationRoleMap) const
{
    for (auto& keeper : keepers) {
        auto roleInfoMap = std::make_shared<GenerationKeeper::WorkerRoleInfoMap>();
        keeper->fillWorkerRoleInfo(*roleInfoMap, onlyActiveRole);
        auto& buildId = keeper->getBuildId();
        multiGenerationRoleMap[buildId] = roleInfoMap;
    }

    auto agentMaintainer = getGlobalAgentMaintainer();
    assert(agentMaintainer);
    auto globalAgentRoleTargetMap = agentMaintainer->getAgentRoleTargetMap();
    if (globalAgentRoleTargetMap) {
        for (auto it : *globalAgentRoleTargetMap) {
            auto& agentRole = it.first;
            auto& agentIdentifier = it.second.first;
            for (auto& role : it.second.second) {
                for (auto [_, roleInfoMap] : multiGenerationRoleMap) {
                    auto roleIt = roleInfoMap->find(role);
                    if (roleIt != roleInfoMap->end()) {
                        // update role agent info
                        roleIt->second.agentRoleName = agentRole;
                        roleIt->second.agentIdentifier = agentIdentifier;
                        break;
                    }
                }
            }
        }
    }
}

void ServiceKeeper::fillWorkerRoleInfo(const proto::WorkerRoleInfoRequest* request,
                                       proto::WorkerRoleInfoResponse* response)
{
    auto matchKeepers = getBuildIdMatchedGenerationKeepers(request->buildid(), true);
    std::map<proto::BuildId, GenerationKeeper::WorkerRoleInfoMapPtr> multiGenerationRoleMap;
    fillWorkerRoleInfoMap(matchKeepers, request->onlyactive(), multiGenerationRoleMap);

    // generate response from roleInfoMap object
    for (auto [id, roleInfoMap] : multiGenerationRoleMap) {
        GenerationRoleInfo* generationRoleInfo = response->add_generationroleinfos();
        *generationRoleInfo->mutable_buildid() = id;
        for (auto roleIt = roleInfoMap->begin(); roleIt != roleInfoMap->end(); roleIt++) {
            WorkerRoleInfo* roleInfo = generationRoleInfo->add_roleinfos();
            roleInfo->set_rolename(roleIt->first);
            roleInfo->set_roletype(roleIt->second.roleType);
            roleInfo->set_identifier(roleIt->second.identifier);
            roleInfo->set_heartbeattime(roleIt->second.heartbeatTime);
            roleInfo->set_workerstarttime(roleIt->second.workerStartTime);
            roleInfo->set_isfinish(roleIt->second.isFinish);
            roleInfo->set_agentnodename(roleIt->second.agentRoleName);
            roleInfo->set_agentidentifier(roleIt->second.agentIdentifier);
        }
    }

    set<BuildId> recoverFailedGenerations;
    {
        ScopedLock lock(_mapLock);
        recoverFailedGenerations = _recoverFailedGenerations;
    }
    for (auto& it : recoverFailedGenerations) {
        if (isBuildIdMatched(request->buildid(), it)) {
            GenerationRoleInfo* generationRoleInfo = response->add_generationroleinfos();
            *generationRoleInfo->mutable_buildid() = it;
            *generationRoleInfo->mutable_errormsg() = it.ShortDebugString() + " recover failed";
        }
    }
}

void ServiceKeeper::startTask(const proto::StartTaskRequest* request, proto::InformResponse* response)
{
    BS_LOG(INFO, "Begin start task [%s].", request->ShortDebugString().c_str());
    string msg = "Begin start task[" + request->ShortDebugString() + "].";
    beeper::EventTags collectTags = BuildIdWrapper::getEventTags(request->buildid());
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);

    kmonitor_adapter::ScopeLatencyReporter kstartReporter(_keeperMetric.kstartTaskLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.kstartTaskQps, 1);

    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);

    const BuildId& buildId = request->buildid();
    bool isRecoverFailed = false;
    GenerationKeeperPtr generationKeeper = getGeneration(buildId, false, isRecoverFailed);
    if (!generationKeeper) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_NOT_FOUND, "active generation: build[%s] not exists!",
                             buildId.ShortDebugString().c_str());
    }
    if (isRecoverFailed) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "build[%s] already exists, but recover failed!",
                             buildId.ShortDebugString().c_str());
    }
    generationKeeper->startTask(request, response);
    if (response->errormessage_size() > 0) {
        return;
    }
    BS_LOG(INFO, "Finish start task[%s].", request->ShortDebugString().c_str());
    msg = "Finish start task[" + request->ShortDebugString() + "].";
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);
}

void ServiceKeeper::stopTask(const proto::StopTaskRequest* request, proto::InformResponse* response)
{
    BS_LOG(INFO, "Begin stop task[%s].", request->ShortDebugString().c_str());
    string msg = "Begin stop task[" + request->ShortDebugString() + "].";
    beeper::EventTags collectTags = BuildIdWrapper::getEventTags(request->buildid());
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);

    REPORT_KMONITOR_METRIC(_keeperMetric.kstopTaskQps, 1);
    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);

    const BuildId& buildId = request->buildid();
    bool isRecoverFailed = false;
    GenerationKeeperPtr generationKeeper = getGeneration(buildId, false, isRecoverFailed, /*fuzzy*/ true);
    if (!generationKeeper) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_NOT_FOUND, "active generation: build[%s] not exists!",
                             buildId.ShortDebugString().c_str());
    }
    if (isRecoverFailed) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "build[%s] already exists, but recover failed!",
                             buildId.ShortDebugString().c_str());
    }

    generationKeeper->stopTask(request, response);
    if (response->errormessage_size() > 0) {
        return;
    }
    BS_LOG(INFO, "Finish stop task[%s].", request->ShortDebugString().c_str());
    msg = "Finish stop task[" + request->ShortDebugString() + "].";
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);
}

void ServiceKeeper::startBuild(const proto::StartBuildRequest* request, proto::StartBuildResponse* response)
{
    kmonitor_adapter::ScopeLatencyReporter kstartReporter(_keeperMetric.kstartBuildLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.kstartBuildQps, 1);

    BS_LOG(INFO, "Begin start build[%s].", request->ShortDebugString().c_str());
    string msg = "Begin start build[" + request->ShortDebugString() + "].";
    beeper::EventTags collectTags = BuildIdWrapper::getEventTags(request->buildid());
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);

    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);

    const BuildId& buildId = request->buildid();
    if (request->has_jobid()) {
        string tmpJobId = buildIdToJobId(buildId);
        if (request->jobid() != tmpJobId) {
            SET_ERROR_AND_RETURN(buildId, response, ADMIN_BAD_REQUEST,
                                 "validate buildid not match with jobId [%s], should be:%s", request->jobid().c_str(),
                                 tmpJobId.c_str());
        }
    }

    {
        // prevent to start the same BuildId twice
        autil::ScopedLock lock(_startBuildLock);
        if (_startingGenerations.find(buildId) != _startingGenerations.end()) {
            BS_LOG(INFO, "buildId[%s] is already in the starting queue.", buildId.ShortDebugString().c_str());
            msg = "buildId[" + buildId.ShortDebugString() + "] is already in the starting queue.";
            BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);
            return;
        }
        _startingGenerations.insert(buildId);
    }
    string buildMode;
    if (request->has_buildmode()) {
        buildMode = request->buildmode();
    }
    ConfigValidator configValidator(buildId);
    if (buildMode != "general_task" && buildMode != "customized" &&
        !configValidator.validate(request->configpath(), true)) {
        autil::ScopedLock lock(_startBuildLock);
        _startingGenerations.erase(buildId);
        string errorInfo = configValidator.getErrorMsg();
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_BAD_REQUEST, "validate config [%s] failed, error msg:%s",
                             request->configpath().c_str(), errorInfo.c_str());
    }

    bool isRecoverFailed = false;
    if (getGeneration(buildId, false, isRecoverFailed)) {
        autil::ScopedLock lock(_startBuildLock);
        _startingGenerations.erase(buildId);
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_ERROR_NONE, "build[%s] already exists!",
                             buildId.ShortDebugString().c_str());
    }
    if (isRecoverFailed) {
        autil::ScopedLock lock(_startBuildLock);
        _startingGenerations.erase(buildId);
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "build[%s] already exists, but recover failed!",
                             buildId.ShortDebugString().c_str());
    }
    GenerationKeeperPtr generationKeeper = createGenerationKeeper(buildId);
    generationKeeper->startBuild(request, response);

    if (response->errormessage_size() > 0) {
        autil::ScopedLock lock(_startBuildLock);
        _startingGenerations.erase(buildId);
        return;
    }
    setGeneration(buildId, generationKeeper);
    {
        autil::ScopedLock lock(_startBuildLock);
        _startingGenerations.erase(buildId);
    }
    if (_healthChecker) {
        if (!_healthChecker->addGenerationKeeper(buildId, generationKeeper)) {
            SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "add build[%s] to HealthChecker failed.",
                                 buildId.ShortDebugString().c_str());
        }
    }
    BS_LOG(INFO, "Finish start build[%s].", request->ShortDebugString().c_str());
    msg = "Finish start build[" + request->ShortDebugString() + "].";
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);
}

bool ServiceKeeper::getGenerationDetailInfoStr(const proto::BuildId& buildid, const std::string& paramStr,
                                               std::string& detailInfoStr) const
{
    auto generateDetailTargets = [](const std::string& paramStr, std::set<std::string>& targets) {
        if (paramStr.empty() || paramStr == "all") {
            targets.insert("task");
            targets.insert("checkpoint");
            targets.insert("graph");
            targets.insert("worker");
            return;
        }
        if (paramStr == "disable_dot_string") {
            targets.insert("task");
            targets.insert("checkpoint");
            targets.insert("brief_graph");
            targets.insert("worker");
            return;
        }
        vector<string> targetVec;
        autil::StringUtil::fromString(paramStr, targetVec, ";");
        targets.insert(targetVec.begin(), targetVec.end());
    };

    set<string> targets;
    generateDetailTargets(paramStr, targets);

    auto matchKeepers = getBuildIdMatchedGenerationKeepers(buildid, true);
    if (matchKeepers.empty()) {
        return false;
    }

    std::map<proto::BuildId, GenerationKeeper::WorkerRoleInfoMapPtr> multiGenerationRoleMap;
    fillWorkerRoleInfoMap(matchKeepers, true, multiGenerationRoleMap);
    autil::legacy::json::JsonArray array;
    for (auto& keeper : matchKeepers) {
        auto& targetBuildId = keeper->getBuildId();
        autil::legacy::json::JsonMap jsonMap;
        fillSummaryInfo(keeper, jsonMap);
        if (targets.find("task") != targets.end()) {
            fillTaskStatusInfo(keeper, jsonMap);
        }
        if (targets.find("checkpoint") != targets.end()) {
            fillCheckpointInfo(keeper, jsonMap);
        }
        if (targets.find("graph") != targets.end()) {
            fillTaskFlowGraphInfo(keeper, /* onlyBriefInfo*/ false, jsonMap);
        }
        if (targets.find("brief_graph") != targets.end()) {
            fillTaskFlowGraphInfo(keeper, /* onlyBriefInfo*/ true, jsonMap);
        }
        if (targets.find("worker") != targets.end()) {
            fillWorkerRoleInfo(multiGenerationRoleMap[targetBuildId], jsonMap);
        }
        array.emplace_back(jsonMap);
    }
    detailInfoStr = autil::legacy::ToJsonString(array);
    return true;
}

void ServiceKeeper::fillSummaryInfo(const GenerationKeeperPtr& keeper, autil::legacy::json::JsonMap& jsonMap) const
{
    assert(keeper);
    auto& bid = keeper->getBuildId();
    autil::legacy::json::JsonMap buildIdInfoMap;
    buildIdInfoMap["generation_id"] = autil::legacy::ToJson(bid.generationid());
    buildIdInfoMap["app_name"] = autil::legacy::ToJson(bid.appname());
    buildIdInfoMap["data_table"] = autil::legacy::ToJson(bid.datatable());

    autil::legacy::json::JsonMap summaryInfoMap;
    summaryInfoMap["build_id"] = buildIdInfoMap;
    summaryInfoMap["current_time"] = autil::legacy::ToJson(autil::TimeUtility::currentTimeString("%Y-%m-%d %H:%M:%S"));
    summaryInfoMap["http_address"] = autil::legacy::ToJson(_httpAddress);
    summaryInfoMap["hippo_zk_root"] = autil::legacy::ToJson(_hippoZkRoot);
    summaryInfoMap["admin_service_name"] = autil::legacy::ToJson(_adminServiceName);
    summaryInfoMap["kmonitor_service_name"] = autil::legacy::ToJson(_kmonServiceName);
    summaryInfoMap["config_path"] = autil::legacy::ToJson(keeper->getConfigPath());
    summaryInfoMap["fatal_error"] = autil::legacy::ToJson(keeper->getFatalErrorString());
    summaryInfoMap["index_root"] = autil::legacy::ToJson(keeper->getIndexRoot());
    summaryInfoMap["build_step"] = autil::legacy::ToJson(getBuildStepString(keeper));
    summaryInfoMap["generation_step"] = autil::legacy::ToJson(getGenerationStepString(keeper));
    summaryInfoMap["generation_zk_root"] = autil::legacy::ToJson(keeper->getGenerationDir());
    summaryInfoMap["generation_type"] = autil::legacy::ToJson(keeper->getGenerationTaskTypeString());
    summaryInfoMap["cluster_names"] = autil::legacy::ToJson(
            autil::StringUtil::toString(keeper->getClusterNames(), ";"));
    auto metricPtr = keeper->getGenerationMetrics();
    if (metricPtr) {
        summaryInfoMap["active_node_count"] = autil::legacy::ToJson(metricPtr->activeNodeCount);
        summaryInfoMap["assign_slot_node_count"] = autil::legacy::ToJson(metricPtr->assignSlotCount);
        summaryInfoMap["wait_slot_node_count"] = autil::legacy::ToJson(metricPtr->waitSlotNodeCount);
        summaryInfoMap["total_release_slow_slot_count"] = autil::legacy::ToJson(metricPtr->totalReleaseSlowSlotCount);
    }
    jsonMap["generation_summary"] = summaryInfoMap;
}

void ServiceKeeper::fillTaskFlowGraphInfo(const GenerationKeeperPtr& keeper, bool onlyBriefInfo,
                                          autil::legacy::json::JsonMap& jsonMap) const
{
    assert(keeper);
    string flowGraphStr;
    map<string, string> subGraphStrMap;
    map<string, map<string, string>> taskDetailInfo;
    keeper->fillGenerationGraphData(flowGraphStr, subGraphStrMap, taskDetailInfo);
    if (onlyBriefInfo) {
        flowGraphStr.clear();
        subGraphStrMap.clear();
    }
    autil::legacy::json::JsonMap graphInfoMap;
    graphInfoMap["flow_graph"] = autil::legacy::ToJson(flowGraphStr);
    graphInfoMap["task_graphs"] = autil::legacy::ToJson(subGraphStrMap);
    graphInfoMap["task_detail"] = autil::legacy::ToJson(taskDetailInfo);
    jsonMap["graph_info"] = graphInfoMap;
}

void ServiceKeeper::fillTaskStatusInfo(const GenerationKeeperPtr& keeper, autil::legacy::json::JsonMap& jsonMap) const
{
    assert(keeper);
    vector<TaskStatusMetricReporter::TaskLogItem> logInfo;
    keeper->fillGenerationTaskNodeStatus(logInfo);

    autil::legacy::json::JsonArray array;
    for (auto& item : logInfo) {
        autil::legacy::json::JsonMap innerMap;
        innerMap["id"] = autil::legacy::ToJson(item.id);
        innerMap["total_count"] = autil::legacy::ToJson(item.totalNodeCount);
        innerMap["running_count"] = autil::legacy::ToJson(item.runningNodeCount);
        innerMap["running_time"] = autil::legacy::ToJson(item.taskRunningTime);
        array.emplace_back(innerMap);
    }
    jsonMap["task_status"] = array;
}

void ServiceKeeper::fillCheckpointInfo(const GenerationKeeperPtr& keeper, autil::legacy::json::JsonMap& jsonMap) const
{
    assert(keeper);
    map<string, vector<pair<string, int64_t>>> infoMap;
    keeper->fillGenerationCheckpointInfo(infoMap);

    autil::legacy::json::JsonMap map;
    for (auto iter = infoMap.begin(); iter != infoMap.end(); iter++) {
        autil::legacy::json::JsonArray array;
        for (auto& data : iter->second) {
            autil::legacy::json::JsonMap innerMap;
            innerMap["id"] = autil::legacy::ToJson(data.first);
            innerMap["value"] = autil::legacy::ToJson(data.second);
            array.emplace_back(innerMap);
        }
        map[iter->first] = array;
    }
    jsonMap["checkpoint_info"] = map;
}

void ServiceKeeper::fillWorkerRoleInfo(const GenerationKeeper::WorkerRoleInfoMapPtr& roleInfoMap,
                                       autil::legacy::json::JsonMap& jsonMap) const
{
    autil::legacy::json::JsonArray array;
    if (!roleInfoMap) {
        jsonMap["worker_role_list"] = array;
        return;
    }
    for (auto iter = roleInfoMap->begin(); iter != roleInfoMap->end(); iter++) {
        auto& roleInfo = iter->second;
        autil::legacy::json::JsonMap roleInfoMap;
        roleInfoMap["role_name"] = autil::legacy::ToJson(iter->first);
        roleInfoMap["type"] = autil::legacy::ToJson(roleInfo.roleType);
        roleInfoMap["identifier"] = autil::legacy::ToJson(roleInfo.identifier);
        roleInfoMap["heartbeat_time"] = autil::legacy::ToJson(roleInfo.heartbeatTime);
        roleInfoMap["worker_start_time"] = autil::legacy::ToJson(roleInfo.workerStartTime);        
        roleInfoMap["is_finish"] = autil::legacy::ToJson(roleInfo.isFinish);
        roleInfoMap["agent_role"] = autil::legacy::ToJson(roleInfo.agentRoleName);
        roleInfoMap["agent_identifier"] = autil::legacy::ToJson(roleInfo.agentIdentifier);
        array.emplace_back(roleInfoMap);
    }
    jsonMap["worker_role_list"] = array;
}

std::vector<GenerationKeeperPtr> ServiceKeeper::getBuildIdMatchedGenerationKeepers(const proto::BuildId& buildid,
                                                                                   bool onlyActive) const
{
    GenerationKeepers keepers;
    {
        autil::ScopedLock lock(_mapLock);
        if (onlyActive) {
            keepers = _activeGenerationKeepers;
        } else {
            keepers = _allGenerationKeepers;
        }
    }
    std::vector<GenerationKeeperPtr> matchKeepers;
    for (GenerationKeepers::const_iterator it = keepers.begin(); it != keepers.end(); ++it) {
        if (isBuildIdMatched(buildid, it->first)) {
            const GenerationKeeperPtr& keeper = it->second;
            matchKeepers.emplace_back(keeper);
        }
    }
    return matchKeepers;
}

std::string ServiceKeeper::getGenerationListInfoStr() const
{
    GenerationKeepers keepers;
    set<BuildId> recoverFailedGenerations;
    {
        autil::ScopedLock lock(_mapLock);
        keepers = _allGenerationKeepers;
        recoverFailedGenerations = _recoverFailedGenerations;
    }
    autil::legacy::json::JsonArray array;
    for (GenerationKeepers::const_iterator it = keepers.begin(); it != keepers.end(); ++it) {
        const BuildId& bid = it->first;
        const GenerationKeeperPtr& keeper = it->second;
        autil::legacy::json::JsonMap infoMap;
        infoMap["generation_id"] = autil::legacy::ToJson(bid.generationid());
        infoMap["app_name"] = autil::legacy::ToJson(bid.appname());
        infoMap["data_table"] = autil::legacy::ToJson(bid.datatable());
        infoMap["build_step"] = autil::legacy::ToJson(getBuildStepString(keeper));
        string statusStr = keeper->isStopped() ? "stopped" : "active";
        infoMap["status"] = autil::legacy::ToJson(statusStr);
        infoMap["generation_type"] = autil::legacy::ToJson(keeper->getGenerationTaskTypeString());
        array.emplace_back(infoMap);
    }
    for (auto& bid : recoverFailedGenerations) {
        autil::legacy::json::JsonMap infoMap;
        infoMap["generation_id"] = autil::legacy::ToJson(bid.generationid());
        infoMap["app_name"] = autil::legacy::ToJson(bid.appname());
        infoMap["data_table"] = autil::legacy::ToJson(bid.datatable());
        infoMap["build_step"] = autil::legacy::ToJson(std::string("unknown"));
        infoMap["status"] = autil::legacy::ToJson(std::string("recover_failed"));
        infoMap["generation_type"] = autil::legacy::ToJson(std::string("unknown"));
        array.emplace_back(infoMap);
    }
    autil::legacy::json::JsonMap jsonMap;
    jsonMap["current_time"] = autil::legacy::ToJson(autil::TimeUtility::currentTimeString("%Y-%m-%d %H:%M:%S"));
    jsonMap["zk_root"] = autil::legacy::ToJson(_zkRoot);
    jsonMap["admin_service_name"] = autil::legacy::ToJson(_adminServiceName);
    jsonMap["http_address"] = autil::legacy::ToJson(_httpAddress);
    jsonMap["hippo_zk_root"] = autil::legacy::ToJson(_hippoZkRoot);
    jsonMap["kmonitor_service_name"] = autil::legacy::ToJson(_kmonServiceName);
    jsonMap["generation_list"] = array;
    return autil::legacy::ToJsonString(jsonMap);
}

void ServiceKeeper::getGeneralInfo(const proto::InformRequest* request, proto::InformResponse* response)
{
    string infoType = request->type();
    auto buildid = request->buildid();
    if (infoType == "generation_list") {
        response->set_response(getGenerationListInfoStr());
        return;
    }
    if (infoType == "generation_detail") {
        if (!request->has_buildid()) {
            SET_ERROR_AND_RETURN(buildid, response, ADMIN_BAD_REQUEST,
                                 "buildId [%s] param is required for generation detail type",
                                 buildid.ShortDebugString().c_str());
            return;
        }
        string detailInfoString;
        if (!getGenerationDetailInfoStr(buildid, request->paramstr(), detailInfoString)) {
            SET_ERROR_AND_RETURN(buildid, response, ADMIN_NOT_FOUND, "buildid [%s] not found in active generations",
                                 buildid.ShortDebugString().c_str());
            return;
        }
        response->set_response(detailInfoString);
        return;
    }
    SET_ERROR_AND_RETURN(buildid, response, ADMIN_BAD_REQUEST, "unknown type [%s]", infoType.c_str());
}

void ServiceKeeper::getBulkloadInfo(const proto::BulkloadInfoRequest* request, proto::InformResponse* response)
{
    BS_LOG(INFO, "Begin get bulkload info [%s].", request->ShortDebugString().c_str());
    std::string msg = "Begin get bulkload info [" + request->ShortDebugString() + "].";
    beeper::EventTags collectTags = BuildIdWrapper::getEventTags(request->buildid());
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);

    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);

    const BuildId& buildId = request->buildid();
    const std::string& clusterName = request->clustername();
    const std::string& bulkloadId = request->bulkloadid();
    const auto& ranges = request->ranges();

    bool isRecoverFailed = false;
    GenerationKeeperPtr generationKeeper = getGeneration(buildId, false, isRecoverFailed);
    if (isRecoverFailed) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                             "getBulkloadInfo failed : [%s] exists, but recover failed.",
                             buildId.ShortDebugString().c_str());
    }
    if (!generationKeeper) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_NOT_FOUND, "getBulkloadInfo failed : [%s] not exists.",
                             buildId.ShortDebugString().c_str());
    }
    std::string errorMsg;
    std::string resultStr;
    if (!generationKeeper->getBulkloadInfo(clusterName, bulkloadId, ranges, &resultStr, errorMsg)) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "getBulkloadInfo [%s] failed for: [%s] ",
                             buildId.ShortDebugString().c_str(), errorMsg.c_str());
    }
    if (!resultStr.empty()) {
        response->set_response(resultStr);
    }
    BS_LOG(INFO, "Finish getBulkloadInfo [%s].", request->ShortDebugString().c_str());
    msg = "Finish getBulkloadInfo [" + request->ShortDebugString() + "].";
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);
}

GenerationKeeperPtr ServiceKeeper::createGenerationKeeper(const BuildId& buildId)
{
    ProhibitedIpCollectorPtr collector;
    if (_prohibitedIpsTable) {
        collector = _prohibitedIpsTable->createCollector();
    }
    string amonPortStr = StringUtil::toString(_amonitorPort);
    return doCreateGenerationKeeper({buildId, _zkRoot, _adminServiceName, amonPortStr, _zkWrapper, collector, _monitor,
                                     /*catalogPartitionIdentifier*/ nullptr, /*catalogPartitionIdentifier*/ nullptr,
                                     _specifiedWorkerPkgList});
}

GenerationKeeperPtr ServiceKeeper::doCreateGenerationKeeper(const GenerationKeeper::Param& param)
{
    GenerationKeeperPtr generationKeeper(new GenerationKeeper(param));
    return generationKeeper;
}

void ServiceKeeper::suspendBuild(const proto::SuspendBuildRequest* request, proto::InformResponse* response)
{
    BS_LOG(INFO, "Begin suspend build [%s].", request->ShortDebugString().c_str());
    string msg = "Begin suspend build [" + request->ShortDebugString() + "].";
    beeper::EventTags collectTags = BuildIdWrapper::getEventTags(request->buildid());
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);

    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);

    const BuildId& buildId = request->buildid();
    bool isRecoverFailed = false;
    GenerationKeeperPtr generationKeeper = getGeneration(buildId, false, isRecoverFailed);
    if (isRecoverFailed) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                             "suspend build failed : [%s] exists, but recover failed.",
                             buildId.ShortDebugString().c_str());
    }
    if (!generationKeeper) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_NOT_FOUND, "suspend build failed : [%s] not exists.",
                             buildId.ShortDebugString().c_str());
    }
    string errorMsg;
    const string& clusterName = request->clustername();
    if (!generationKeeper->suspendBuild(clusterName, errorMsg)) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "suspend build [%s] failed for: [%s] ",
                             buildId.ShortDebugString().c_str(), errorMsg.c_str());
    }
    BS_LOG(INFO, "Finish suspend build [%s].", request->ShortDebugString().c_str());
    msg = "Finish suspend build [" + request->ShortDebugString() + "].";
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);
}

void ServiceKeeper::resumeBuild(const proto::ResumeBuildRequest* request, proto::InformResponse* response)
{
    BS_LOG(INFO, "Begin resume build [%s].", request->ShortDebugString().c_str());
    string msg = "Begin resume build [" + request->ShortDebugString() + "].";
    beeper::EventTags collectTags = BuildIdWrapper::getEventTags(request->buildid());
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);

    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);

    const BuildId& buildId = request->buildid();
    bool isRecoverFailed = false;
    GenerationKeeperPtr generationKeeper = getGeneration(buildId, false, isRecoverFailed);
    if (isRecoverFailed) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                             "resume build failed : [%s] exists, but recover failed.",
                             buildId.ShortDebugString().c_str());
    }
    if (!generationKeeper) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_NOT_FOUND, "resume build failed : [%s] not exists.",
                             buildId.ShortDebugString().c_str());
    }
    string errorMsg;
    const string& clusterName = request->clustername();
    if (!generationKeeper->resumeBuild(clusterName, errorMsg)) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "resume build [%s] failed for: [%s] ",
                             buildId.ShortDebugString().c_str(), errorMsg.c_str());
    }
    BS_LOG(INFO, "Finish resume build [%s].", request->ShortDebugString().c_str());
    msg = "Finish resume build [" + request->ShortDebugString() + "].";
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);
}

void ServiceKeeper::rollBack(const proto::RollBackRequest* request, proto::InformResponse* response)
{
    BS_LOG(INFO, "Begin rollBack [%s].", request->ShortDebugString().c_str());
    string msg = "Begin rollBack [" + request->ShortDebugString() + "].";
    beeper::EventTags collectTags = BuildIdWrapper::getEventTags(request->buildid());
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);

    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);

    const BuildId& buildId = request->buildid();
    const string& clusterName = request->clustername();
    const versionid_t targetVersion = request->versionid();
    int64_t startTimestamp = request->starttimestamp();

    bool isRecoverFailed = false;
    GenerationKeeperPtr generationKeeper = getGeneration(buildId, false, isRecoverFailed);
    if (isRecoverFailed) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                             "rollBack failed : [%s] exists, but recover failed.", buildId.ShortDebugString().c_str());
    }
    if (!generationKeeper) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_NOT_FOUND, "rollBack failed : [%s] not exists.",
                             buildId.ShortDebugString().c_str());
    }
    string errorMsg;
    if (!generationKeeper->rollBack(clusterName, targetVersion, startTimestamp, errorMsg)) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "rollBack [%s] failed for: [%s] ",
                             buildId.ShortDebugString().c_str(), errorMsg.c_str());
    }
    BS_LOG(INFO, "Finish rollback [%s].", request->ShortDebugString().c_str());
    msg = "Finish rollBack [" + request->ShortDebugString() + "].";
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);
}

void ServiceKeeper::rollBackCheckpoint(const proto::RollBackCheckpointRequest* request, proto::InformResponse* response)
{
    BS_LOG(INFO, "Begin rollBack checkpoint [%s].", request->ShortDebugString().c_str());
    string msg = "Begin rollBack checkpoint [" + request->ShortDebugString() + "].";
    beeper::EventTags collectTags = BuildIdWrapper::getEventTags(request->buildid());
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);

    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);

    const BuildId& buildId = request->buildid();
    const string& clusterName = request->clustername();
    const checkpointid_t targetCheckpointId = request->checkpointid();
    const auto& ranges = request->ranges();

    bool isRecoverFailed = false;
    GenerationKeeperPtr generationKeeper = getGeneration(buildId, false, isRecoverFailed);
    if (isRecoverFailed) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                             "rollBack failed : [%s] exists, but recover failed.", buildId.ShortDebugString().c_str());
    }
    if (!generationKeeper) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_NOT_FOUND, "rollBack failed : buildId[%s] not exists.",
                             buildId.ShortDebugString().c_str());
    }
    string errorMsg;
    if (!generationKeeper->rollBackCheckpoint(clusterName, targetCheckpointId, ranges, errorMsg)) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "rollBack checkpoint [%s] failed for: [%s] ",
                             buildId.ShortDebugString().c_str(), errorMsg.c_str());
    }
    BS_LOG(INFO, "Finish rollback checkpoint [%s].", request->ShortDebugString().c_str());
    msg = "Finish rollBack checkpoint [" + request->ShortDebugString() + "].";
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);
}

void ServiceKeeper::createSavepoint(const proto::CreateSavepointRequest* request, proto::InformResponse* response)
{
    BS_LOG(INFO, "Begin createSavepoint [%s].", request->ShortDebugString().c_str());
    string msg = "Begin createSavepoint [" + request->ShortDebugString() + "].";
    beeper::EventTags collectTags = BuildIdWrapper::getEventTags(request->buildid());
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);

    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);

    const BuildId& buildId = request->buildid();
    const string& clusterName = request->clustername();
    const checkpointid_t checkpointId = request->savepointid();
    const string& comment = request->comment();

    bool isRecoverFailed = false;
    std::shared_ptr<GenerationKeeper> generationKeeper =
        getGeneration(buildId, /*includeHistory=*/false, isRecoverFailed);
    if (isRecoverFailed) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                             "createSavepoint failed : [%s] exists, but recover failed.",
                             buildId.ShortDebugString().c_str());
    }
    if (!generationKeeper) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_NOT_FOUND, "createSavepoint failed : [%s] not exists.",
                             buildId.ShortDebugString().c_str());
    }
    string errorMsg;
    string savepointStr;
    if (!generationKeeper->createSavepoint(clusterName, checkpointId, comment, savepointStr, errorMsg)) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "createSavepoint [%s] failed for: [%s] ",
                             buildId.ShortDebugString().c_str(), errorMsg.c_str());
    }
    if (!savepointStr.empty()) {
        response->set_response(savepointStr);
    }
    BS_LOG(INFO, "Finish createSavepoint [%s].", request->ShortDebugString().c_str());
    msg = "Finish createSavepoint [" + request->ShortDebugString() + "].";
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);
}

void ServiceKeeper::removeSavepoint(const proto::RemoveSavepointRequest* request, proto::InformResponse* response)
{
    BS_LOG(INFO, "Begin removeSavepoint [%s].", request->ShortDebugString().c_str());
    string msg = "Begin removeSavepoint [" + request->ShortDebugString() + "].";
    beeper::EventTags collectTags = BuildIdWrapper::getEventTags(request->buildid());
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);

    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);

    const BuildId& buildId = request->buildid();
    const string& clusterName = request->clustername();
    const checkpointid_t checkpointId = request->savepointid();

    bool isRecoverFailed = false;
    GenerationKeeperPtr generationKeeper = getGeneration(buildId, /*includeHistory=*/false, isRecoverFailed);
    if (isRecoverFailed) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                             "removeSavepoint failed : [%s] exists, but recover failed.",
                             buildId.ShortDebugString().c_str());
    }
    if (!generationKeeper) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_NOT_FOUND, "removeSavepoint failed : [%s] not exists.",
                             buildId.ShortDebugString().c_str());
    }
    string errorMsg;
    if (!generationKeeper->removeSavepoint(clusterName, checkpointId, errorMsg)) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "removeSavepoint [%s] failed for: [%s] ",
                             buildId.ShortDebugString().c_str(), errorMsg.c_str());
    }
    BS_LOG(INFO, "Finish removeSavepoint [%s].", request->ShortDebugString().c_str());
    msg = "Finish removeSavepoint [" + request->ShortDebugString() + "].";
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);
}

void ServiceKeeper::listCheckpoint(const proto::ListCheckpointRequest* request, proto::InformResponse* response)
{
    BS_LOG(INFO, "Begin listCheckpoint [%s].", request->ShortDebugString().c_str());
    string msg = "Begin listCheckpoint [" + request->ShortDebugString() + "].";
    beeper::EventTags collectTags = BuildIdWrapper::getEventTags(request->buildid());
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);

    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);

    const BuildId& buildId = request->buildid();
    const string& clusterName = request->clustername();
    const bool savepointOnly = request->savepointonly();
    const uint32_t limit = request->limit();
    const uint32_t offset = request->offset();

    bool isRecoverFailed = false;
    GenerationKeeperPtr generationKeeper = getGeneration(buildId, /*includeHistory=*/false, isRecoverFailed);
    if (isRecoverFailed) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                             "listCheckpoint failed : [%s] exists, but recover failed.",
                             buildId.ShortDebugString().c_str());
    }
    if (!generationKeeper) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_NOT_FOUND, "listCheckpoint failed : buildId [%s] not exists.",
                             buildId.ShortDebugString().c_str());
    }
    string errorMsg;
    std::string result;
    if (!generationKeeper->listCheckpoint(clusterName, savepointOnly, offset, limit, &result)) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "listCheckpoint [%s] failed for: [%s] ",
                             buildId.ShortDebugString().c_str(), errorMsg.c_str());
    }
    if (!result.empty()) {
        response->set_response(result);
    }
    BS_LOG(INFO, "Finish listCheckpoint [%s].", request->ShortDebugString().c_str());
    msg = "Finish listCheckpoint [" + request->ShortDebugString() + "].";
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);
}

void ServiceKeeper::getCheckpoint(const proto::GetCheckpointRequest* request, proto::InformResponse* response)
{
    BS_LOG(INFO, "Begin getCheckpoint [%s].", request->ShortDebugString().c_str());
    string msg = "Begin getCheckpoint [" + request->ShortDebugString() + "].";
    beeper::EventTags collectTags = BuildIdWrapper::getEventTags(request->buildid());
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);

    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);

    const BuildId& buildId = request->buildid();
    const string& clusterName = request->clustername();
    const checkpointid_t checkpointId = request->checkpointid();

    bool isRecoverFailed = false;
    GenerationKeeperPtr generationKeeper = getGeneration(buildId, /*includeHistory=*/false, isRecoverFailed);
    if (isRecoverFailed) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                             "getCheckpoint failed : [%s] exists, but recover failed.",
                             buildId.ShortDebugString().c_str());
    }
    if (!generationKeeper) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_NOT_FOUND, "getCheckpoint failed : [%s] not exists.",
                             buildId.ShortDebugString().c_str());
    }
    string errorMsg;
    std::string result;
    if (!generationKeeper->getCheckpoint(clusterName, checkpointId, &result)) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "getCheckpoint [%s] failed for: [%s] ",
                             buildId.ShortDebugString().c_str(), errorMsg.c_str());
    }
    if (!result.empty()) {
        response->set_response(result);
    }
    BS_LOG(INFO, "Finish getCheckpoint [%s].", request->ShortDebugString().c_str());
    msg = "Finish getCheckpoint [" + request->ShortDebugString() + "].";
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);
}

std::vector<std::pair<GenerationKeeperPtr, proto::BuildId>>
findPrincipalGeneration(const std::map<proto::BuildId, GenerationKeeperPtr>& keepers,
                        const proto::BuildId& requestBuildId)
{
    std::vector<std::pair<GenerationKeeperPtr, proto::BuildId>> ret;
    for (auto [buildId, keeper] : keepers) {
        if (requestBuildId.has_datatable() && requestBuildId.datatable() != buildId.datatable()) {
            continue;
        }
        if (requestBuildId.generationid() != buildId.generationid()) {
            continue;
        }
        if (keeper->getGenerationTaskType() == GenerationTaskBase::TT_GENERAL) {
            continue;
        }
        ret.emplace_back(keeper, buildId);
    }
    if (ret.size() == 1) {
        // compatible with legacy code, because old code, not fill appname
        return ret;
    }
    std::vector<std::pair<GenerationKeeperPtr, proto::BuildId>> finalRet;
    for (auto [keeper, buildId] : ret) {
        if (buildId.appname() != proto::ProtoUtil::getOriginalAppName(requestBuildId.appname())) {
            continue;
        }
        finalRet.emplace_back(keeper, buildId);
    }
    return finalRet;
}

void ServiceKeeper::commitVersion(const proto::CommitVersionRequest* request, proto::InformResponse* response)
{
    BS_LOG(INFO, "Begin commitVersion [%s].", request->ShortDebugString().c_str());
    string msg = "Begin commitVersion [" + request->ShortDebugString() + "].";
    beeper::EventTags collectTags = BuildIdWrapper::getEventTags(request->buildid());
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);

    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);

    GenerationKeepers keepers;
    {
        ScopedLock lock(_mapLock);
        keepers = _activeGenerationKeepers;
    }
    const auto& requestBuildId = request->buildid();
    if (!requestBuildId.has_generationid()) {
        SET_ERROR_AND_RETURN(requestBuildId, response, ADMIN_BAD_REQUEST, "generation id not found in [%s]",
                             requestBuildId.ShortDebugString().c_str());
    }
    auto principalGenerations = findPrincipalGeneration(keepers, requestBuildId);
    if (principalGenerations.size() != 1) {
        SET_ERROR_AND_RETURN(requestBuildId, response, ADMIN_BAD_REQUEST, "find [%lu] generation(s) with id[%d], not 1",
                             principalGenerations.size(), requestBuildId.generationid());
    }

    auto [generationKeeper, principalBuildId] = principalGenerations[0];

    const string& clusterName = request->clustername();
    const proto::Range range = request->range();
    const string versionMeta = request->versionmeta();
    const bool instantPublish = request->instantpublish();
    std::string result;
    std::string errorMsg;
    if (!generationKeeper->commitVersion(clusterName, range, versionMeta, instantPublish, errorMsg)) {
        SET_ERROR_AND_RETURN(requestBuildId, response, ADMIN_INTERNAL_ERROR, "commitVersion [%s] failed for: [%s] ",
                             requestBuildId.ShortDebugString().c_str(), errorMsg.c_str());
    }
    if (!result.empty()) {
        response->set_response(result);
    }
    BS_LOG(INFO, "Finish commitVersion [%s].", request->ShortDebugString().c_str());
    msg = "Finish commitVersion [" + request->ShortDebugString() + "].";
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);
}

void ServiceKeeper::getCommittedVersions(const proto::GetCommittedVersionsRequest* request,
                                         proto::InformResponse* response)
{
    BS_LOG(INFO, "Begin getCommittedVersions [%s].", request->ShortDebugString().c_str());
    string msg = "Begin getCommittedVersions [" + request->ShortDebugString() + "].";
    beeper::EventTags collectTags = BuildIdWrapper::getEventTags(request->buildid());
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);

    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);

    GenerationKeepers keepers;
    {
        ScopedLock lock(_mapLock);
        keepers = _activeGenerationKeepers;
    }
    const auto& requestBuildId = request->buildid();
    if (!requestBuildId.has_generationid()) {
        SET_ERROR_AND_RETURN(requestBuildId, response, ADMIN_BAD_REQUEST, "generation id not found in [%s]",
                             requestBuildId.ShortDebugString().c_str());
    }
    auto principalGenerations = findPrincipalGeneration(keepers, requestBuildId);
    if (principalGenerations.size() != 1) {
        SET_ERROR_AND_RETURN(requestBuildId, response, ADMIN_BAD_REQUEST, "find [%lu] generation(s) with id[%d], not 1",
                             principalGenerations.size(), requestBuildId.generationid());
    }

    auto [generationKeeper, principalBuildId] = principalGenerations[0];

    const string& clusterName = request->clustername();
    const proto::Range range = request->range();
    const uint32_t limit = request->limit();
    std::string result;
    std::string errorMsg;
    if (!generationKeeper->getCommittedVersions(clusterName, range, limit, result, errorMsg)) {
        SET_ERROR_AND_RETURN(requestBuildId, response, ADMIN_INTERNAL_ERROR, "commitVersion [%s] failed for: [%s] ",
                             requestBuildId.ShortDebugString().c_str(), errorMsg.c_str());
    }
    if (!result.empty()) {
        response->set_response(result);
    }
    BS_LOG(INFO, "Finish commitVersion [%s].", request->ShortDebugString().c_str());
    msg = "Finish commitVersion [" + request->ShortDebugString() + "].";
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);
}

void ServiceKeeper::createSnapshot(const proto::CreateSnapshotRequest* request, proto::InformResponse* response)
{
    BS_LOG(INFO, "Begin createSnapshot [%s].", request->ShortDebugString().c_str());
    string msg = "Begin createSnapshot [" + request->ShortDebugString() + "].";
    beeper::EventTags collectTags = BuildIdWrapper::getEventTags(request->buildid());
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);

    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);

    const BuildId& buildId = request->buildid();
    const string& clusterName = request->clustername();
    const versionid_t targetVersion = request->versionid();

    bool isRecoverFailed = false;
    GenerationKeeperPtr generationKeeper = getGeneration(buildId, false, isRecoverFailed);
    if (isRecoverFailed) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                             "createSnapshot failed : [%s] exists, but recover failed.",
                             buildId.ShortDebugString().c_str());
    }
    if (!generationKeeper) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_NOT_FOUND, "createSnapshot failed : [%s] not exists.",
                             buildId.ShortDebugString().c_str());
    }
    string errorMsg;
    if (!generationKeeper->markCheckpoint(clusterName, targetVersion, true, errorMsg)) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "createSnapshot [%s] failed for: [%s] ",
                             buildId.ShortDebugString().c_str(), errorMsg.c_str());
    }
    BS_LOG(INFO, "Finish createSnapshot [%s].", request->ShortDebugString().c_str());
    msg = "Finish createSnapshot [" + request->ShortDebugString() + "].";
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);
}

void ServiceKeeper::removeSnapshot(const proto::RemoveSnapshotRequest* request, proto::InformResponse* response)
{
    BS_LOG(INFO, "Begin removeSnapshot [%s].", request->ShortDebugString().c_str());
    string msg = "Begin removeSnapshot [" + request->ShortDebugString() + "].";
    beeper::EventTags collectTags = BuildIdWrapper::getEventTags(request->buildid());
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);

    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);

    const BuildId& buildId = request->buildid();
    const string& clusterName = request->clustername();
    const versionid_t targetVersion = request->versionid();

    bool isRecoverFailed = false;
    GenerationKeeperPtr generationKeeper = getGeneration(buildId, false, isRecoverFailed);
    if (isRecoverFailed) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                             "removeSnapshot failed : [%s] exists, but recover failed.",
                             buildId.ShortDebugString().c_str());
    }
    if (!generationKeeper) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_NOT_FOUND, "removeSnapshot failed : [%s] not exists.",
                             buildId.ShortDebugString().c_str());
    }
    string errorMsg;
    if (!generationKeeper->markCheckpoint(clusterName, targetVersion, false, errorMsg)) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "removeSnapshot [%s] failed for: [%s] ",
                             buildId.ShortDebugString().c_str(), errorMsg.c_str());
    }
    BS_LOG(INFO, "Finish removeSnapshot [%s].", request->ShortDebugString().c_str());
    msg = "Finish removeSnapshot [" + request->ShortDebugString() + "].";
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);
}

void ServiceKeeper::stopBuild(const proto::StopBuildRequest* request, proto::InformResponse* response)
{
    REPORT_KMONITOR_METRIC(_keeperMetric.kstopBuildQps, 1);
    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);

    const BuildId& buildId = getBuildId(request);

    BS_LOG(INFO, "Begin stop build[%s].", buildId.ShortDebugString().c_str());
    string msg = "Begin stop build [" + buildId.ShortDebugString() + "].";
    beeper::EventTags collectTags = BuildIdWrapper::getEventTags(buildId);
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);

    vector<GenerationKeeperPtr> keepers;
    if (!getActiveGenerationsToStop(buildId, keepers)) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "stop build failed : [%s] recover failed",
                             buildId.ShortDebugString().c_str());
    } else if (keepers.empty()) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_NOT_FOUND, "stop build failed : [%s] not exists.",
                             buildId.ShortDebugString().c_str());
    }
    vector<GenerationKeeperPtr> attachedKeepers;
    {
        autil::ScopedLock lock(_mapLock);
        for (const auto& keeper : keepers) {
            if (keeper->getGenerationTaskType() != GenerationTaskBase::TT_GENERAL) {
                auto attachedGenerations = getAttachedGenerations(keeper->getBuildId());
                attachedKeepers.insert(attachedKeepers.end(), attachedGenerations.begin(), attachedGenerations.end());
            }
        }
    }
    keepers.insert(keepers.end(), attachedKeepers.begin(), attachedKeepers.end());

    uint32_t failedCount = 0;
    for (size_t i = 0; i < keepers.size(); i++) {
        const GenerationKeeperPtr& keeper = keepers[i];
        if (!keeper->stopBuild()) {
            failedCount++;
            BS_LOG(WARN, "stop build[%s] failed", keeper->getBuildId().ShortDebugString().c_str());
            continue;
        }
    }

    if (failedCount > 0) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "not all generations stop successfully for [%s]",
                             buildId.ShortDebugString().c_str());
    }

    BS_LOG(INFO, "Finish stop build[%s].", buildId.ShortDebugString().c_str());
    msg = "Finish stop build [" + buildId.ShortDebugString() + "].";
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);
}

void ServiceKeeper::updateConfig(const proto::UpdateConfigRequest* request, proto::InformResponse* response)
{
    auto buildId = getBuildId(request);
    BS_LOG(INFO, "Begin update config [%s].", request->ShortDebugString().c_str());
    string msg = "Begin update config [" + request->ShortDebugString() + "].";
    beeper::EventTags collectTags = BuildIdWrapper::getEventTags(buildId);
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);

    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);
    bool isRecoverFailed = false;
    GenerationKeeperPtr generationKeeper = getGeneration(buildId, false, isRecoverFailed);
    if (isRecoverFailed) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                             "update config failed : [%s] exists, but recover failed.",
                             buildId.ShortDebugString().c_str());
    }
    if (!generationKeeper) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_NOT_FOUND, "update config failed : [%s] not exists.",
                             buildId.ShortDebugString().c_str());
    }
    string errorInfo;
    if (!generationKeeper->validateConfig(request->configpath(), errorInfo, false)) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_BAD_REQUEST, "validate config [%s] failed, error msg: %s",
                             request->configpath().c_str(), errorInfo.c_str());
    }
    if (!generationKeeper->updateConfig(request->configpath())) {
        std::vector<ErrorInfo> errorInfos;
        generationKeeper->transferErrorInfos(errorInfos);
        string errorMsg = ErrorCollector::errorInfosToString(errorInfos);
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                             "updateConfig failed : [%s] "
                             "error msg:%s",
                             buildId.ShortDebugString().c_str(), errorMsg.c_str());
    }
    std::vector<GenerationKeeperPtr> attachedGenerations;
    {
        autil::ScopedLock lock(_mapLock);
        attachedGenerations = getAttachedGenerations(buildId);
    }

    for (const auto& keeper : attachedGenerations) {
        std::string errorInfo;
        if (!keeper->validateConfig(request->configpath(), errorInfo, /*firstTime*/ false)) {
            SET_ERROR_AND_RETURN(keeper->getBuildId(), response, ADMIN_BAD_REQUEST,
                                 "validate config [%s] failed, error msg: %s", request->configpath().c_str(),
                                 errorInfo.c_str());
        }
        if (!keeper->updateConfig(request->configpath())) {
            std::vector<ErrorInfo> errorInfos;
            keeper->transferErrorInfos(errorInfos);
            string errorMsg = ErrorCollector::errorInfosToString(errorInfos);
            SET_ERROR_AND_RETURN(keeper->getBuildId(), response, ADMIN_INTERNAL_ERROR,
                                 "updateConfig failed : [%s] "
                                 "error msg:%s",
                                 keeper->getBuildId().ShortDebugString().c_str(), errorMsg.c_str());
        }
    }
    BS_LOG(INFO, "Finish update config [%s].", request->ShortDebugString().c_str());
    msg = "Finish update config [" + request->ShortDebugString() + "].";
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);
}

void ServiceKeeper::stepBuild(const proto::StepBuildRequest* request, proto::InformResponse* response)
{
    BS_LOG(INFO, "Begin step build [%s].", request->ShortDebugString().c_str());
    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);

    const BuildId& buildId = request->buildid();
    bool isRecoverFailed = false;
    GenerationKeeperPtr generationKeeper = getGeneration(buildId, false, isRecoverFailed);
    if (isRecoverFailed) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                             "step build failed : [%s] exists, but recover failed.",
                             buildId.ShortDebugString().c_str());
    }
    if (!generationKeeper) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_NOT_FOUND, "step build failed : [%s] not exists.",
                             buildId.ShortDebugString().c_str());
    }
    generationKeeper->stepBuild(response);
    BS_LOG(INFO, "Finish step build [%s].", request->ShortDebugString().c_str());
}

void ServiceKeeper::createVersion(const proto::CreateVersionRequest* request, proto::InformResponse* response)
{
    BS_LOG(INFO, "Begin create version [%s].", request->ShortDebugString().c_str());
    string msg = "Begin create version [" + request->ShortDebugString() + "].";
    beeper::EventTags collectTags = BuildIdWrapper::getEventTags(request->buildid());
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);

    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);

    const BuildId& buildId = request->buildid();
    bool isRecoverFailed = false;
    GenerationKeeperPtr generationKeeper = getGeneration(buildId, false, isRecoverFailed);
    if (isRecoverFailed) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                             "create version failed : [%s] exists, but recover failed.",
                             buildId.ShortDebugString().c_str());
    }
    if (!generationKeeper) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_NOT_FOUND, "create version failed : [%s] not exists.",
                             buildId.ShortDebugString().c_str());
    }
    if (!generationKeeper->createVersion(request->clustername(), request->mergeconfig())) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "create version [%s] failed",
                             buildId.ShortDebugString().c_str());
    }

    BS_LOG(INFO, "Finish create version [%s].", request->ShortDebugString().c_str());
    msg = "Finish create version [" + request->ShortDebugString() + "].";
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);
}

void ServiceKeeper::switchBuild(const proto::SwitchBuildRequest* request, proto::InformResponse* response)
{
    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);

    const BuildId& buildId = request->buildid();
    string msg = "switchBuild [" + buildId.ShortDebugString() + "].";
    beeper::EventTags collectTags = BuildIdWrapper::getEventTags(buildId);
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);

    bool isRecoverFailed = false;
    GenerationKeeperPtr generationKeeper = getGeneration(buildId, false, isRecoverFailed);
    if (isRecoverFailed) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                             "switch build failed : [%s] exists, but recover failed",
                             buildId.ShortDebugString().c_str());
    }
    if (!generationKeeper) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_NOT_FOUND, "switch build failed : [%s] not exists.",
                             buildId.ShortDebugString().c_str());
    }
    if (!generationKeeper->switchBuild()) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "switch build failed for [%s]",
                             buildId.ShortDebugString().c_str());
    }
}

void ServiceKeeper::callGraph(const proto::CallGraphRequest* request, proto::InformResponse* response)
{
    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);

    string msg = "Begin call graph [" + request->ShortDebugString() + "].";
    beeper::EventTags collectTags = BuildIdWrapper::getEventTags(request->buildid());
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);

    const BuildId& buildId = request->buildid();
    bool isRecoverFailed = false;
    GenerationKeeperPtr generationKeeper = getGeneration(buildId, false, isRecoverFailed);
    if (isRecoverFailed) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                             "call graph failed : [%s] exists, but recover failed", buildId.ShortDebugString().c_str());
    }
    if (!generationKeeper) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_NOT_FOUND, "call graph failed : [%s] not exists.",
                             buildId.ShortDebugString().c_str());
    }

    generationKeeper->callGraph(request, response);
    if (response->errormessage_size() > 0) {
        return;
    }
    BS_LOG(INFO, "Finish call graph [%s].", request->ShortDebugString().c_str());
    msg = "Finish call graph [" + request->ShortDebugString() + "].";
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);
}

void ServiceKeeper::printGraph(const proto::PrintGraphRequest* request, proto::InformResponse* response)
{
    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);

    const BuildId& buildId = request->buildid();
    bool isRecoverFailed = false;
    GenerationKeeperPtr generationKeeper = getGeneration(buildId, false, isRecoverFailed);
    if (isRecoverFailed) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                             "print graph failed : [%s] exists, but recover failed",
                             buildId.ShortDebugString().c_str());
    }
    if (!generationKeeper) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_NOT_FOUND, "print graph failed : [%s] not exists.",
                             buildId.ShortDebugString().c_str());
    }

    generationKeeper->printGraph(request, response);
    if (response->errormessage_size() > 0) {
        return;
    }
    BS_LOG(INFO, "Finish print graph [%s].", request->ShortDebugString().c_str());
}

void ServiceKeeper::updateGsTemplate(const proto::UpdateGsTemplateRequest* request, proto::InformResponse* response)
{
    proto::BuildId buildid;
    string templateStr;
    if (request->has_templatestr()) {
        templateStr = request->templatestr();
    } else {
        if (request->has_templatefile()) {
            string templateFile = request->templatefile();
            if (!fslib::util::FileUtil::readFile(templateFile, templateStr)) {
                SET_ERROR_AND_RETURN(buildid, response, ADMIN_BAD_REQUEST,
                                     "Read gsInfo template file failed, templateFile[%s]", templateFile.c_str());
            }
        }
    }
    if (!_serviceInfoFilter.update(templateStr)) {
        SET_ERROR_AND_RETURN(buildid, response, ADMIN_INTERNAL_ERROR,
                             "Update gs template failed, no templateStr or templateStr illegal:[%s]",
                             templateStr.c_str());
    }
    return;
}

void ServiceKeeper::updateGlobalAgentConfig(const proto::UpdateGlobalAgentConfigRequest* request,
                                            proto::InformResponse* response)
{
    proto::BuildId buildid;
    string configStr;
    if (request->has_configstr()) {
        configStr = request->configstr();
    } else {
        if (request->has_configfile()) {
            string configFile = request->configfile();
            if (!fslib::util::FileUtil::readFile(configFile, configStr)) {
                SET_ERROR_AND_RETURN(buildid, response, ADMIN_BAD_REQUEST,
                                     "Read global agent resource config file failed, configFile[%s]",
                                     configFile.c_str());
            }
        }
    }
    autil::ScopedLock lock(_updateGlobalAgentConfigLock);
    if (!updateGlobalAgentConfig(configStr)) {
        SET_ERROR_AND_RETURN(buildid, response, ADMIN_INTERNAL_ERROR,
                             "Update global agent resource config failed, config string:[%s]", configStr.c_str());
    }
}

bool ServiceKeeper::updateGlobalAgentNodeCount(
    const std::shared_ptr<GlobalAgentMaintainer>& targetAgentMaintainer,
    const std::map<std::pair<std::string, std::string>, uint32_t>& agentGroupCountMap)
{
    autil::ScopedLock lock(_updateGlobalAgentConfigLock);
    if (agentGroupCountMap.empty()) { // no need to update
        return true;
    }
    auto agentMaintainer = getGlobalAgentMaintainer();
    if (!agentMaintainer) {
        return false;
    }
    if (agentMaintainer.get() != targetAgentMaintainer.get()) {
        BS_LOG(WARN, "different global agent maintainer object, ignore current flexible update plan.");
        return false;
    }
    std::string newConfigStr;
    if (!generateNewGlobalAgentConfig(agentMaintainer, agentGroupCountMap, newConfigStr)) {
        return false;
    }
    return updateGlobalAgentConfig(newConfigStr);
}

bool ServiceKeeper::generateNewGlobalAgentConfig(
    const std::shared_ptr<GlobalAgentMaintainer>& agentMaintainer,
    const std::map<std::pair<std::string, std::string>, uint32_t>& agentGroupCountMap, std::string& newConfigStr)
{
    assert(agentMaintainer);
    auto& flexConfigs = agentMaintainer->getFlexibleScaleConfigs();
    map<string, int64_t> updateLog = agentMaintainer->getFlexibleAgentUpdateLog();
    autil::legacy::json::JsonArray jsonArray;
    auto targetCountMap = agentGroupCountMap;
    for (auto singleGroup : agentMaintainer->getGlobalAgentGroups()) {
        assert(singleGroup);
        autil::legacy::json::JsonMap jsonMap;
        try {
            autil::legacy::Any a = autil::legacy::json::ParseJson(singleGroup->getConfigJsonString());
            jsonMap = autil::legacy::AnyCast<autil::legacy::json::JsonMap>(a);
        } catch (const autil::legacy::ExceptionBase& e) {
            string errorMsg =
                "jsonize " + singleGroup->getConfigJsonString() + " failed, exception: " + string(e.what());
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        auto globalId = singleGroup->getAgentGroupName();
        auto agentConfigs = singleGroup->getAgentConfigs();
        bool changed = false;
        for (auto& config : agentConfigs) {
            auto key = std::make_pair(globalId, config.identifier);
            auto iter = targetCountMap.find(key);
            if (iter == targetCountMap.end()) {
                continue;
            }
            string targetKey = GlobalAgentMaintainer::FlexibleScaleConfig::getKey(key.first, key.second);
            updateLog[targetKey] = autil::TimeUtility::currentTimeInSeconds();
            if (config.agentNodeCount == iter->second) {
                targetCountMap.erase(key);
                continue;
            }
            BS_LOG(INFO, "update agentNodeCount from [%u] to [%u] for global agent group [%s|%s]",
                   config.agentNodeCount, iter->second, globalId.c_str(), config.identifier.c_str());
            config.agentNodeCount = iter->second;
            changed = true;
            targetCountMap.erase(key);
        }
        if (changed) {
            jsonMap[AgentGroupConfig::AGENT_CONFIG_GROUP_KEY] = autil::legacy::ToJson(agentConfigs);
        }
        jsonArray.push_back(jsonMap);
    }
    for (auto& item : targetCountMap) {
        BS_LOG(WARN, "update agentNodeCount for [%s|%s] fail, target not found!", item.first.first.c_str(),
               item.first.second.c_str());
    }
    autil::legacy::json::JsonMap newJsonMap;
    newJsonMap[GlobalAgentMaintainer::GLOBAL_AGENT_CONFIG_KEY] = jsonArray;
    newJsonMap[GlobalAgentMaintainer::FLEX_SCALE_CONFIG_KEY] = autil::legacy::ToJson(flexConfigs);
    newJsonMap[GlobalAgentMaintainer::FLEX_UPDATE_LOG_KEY] = autil::legacy::ToJson(updateLog);
    newConfigStr = autil::legacy::json::ToString(newJsonMap);
    return true;
}

bool ServiceKeeper::updateGlobalAgentConfig(const std::string& configStr)
{
    string amonPortStr = StringUtil::toString(_amonitorPort);
    auto newMaintainer =
        std::make_shared<GlobalAgentMaintainer>(_zkWrapper, _zkRoot, _adminServiceName, amonPortStr, _monitor);
    if (!newMaintainer->loadFromString(configStr)) {
        return false;
    }
    setGlobalAgentMaintainer(newMaintainer);
    return true;
}

void ServiceKeeper::startGeneralTask(const proto::StartGeneralTaskRequest* request, proto::InformResponse* response)
{
    GenerationKeepers keepers;
    {
        ScopedLock lock(_mapLock);
        keepers = _activeGenerationKeepers;
    }
    const auto& requestBuildId = request->buildid();
    if (!requestBuildId.has_generationid()) {
        SET_ERROR_AND_RETURN(requestBuildId, response, ADMIN_BAD_REQUEST, "generation id not found in [%s]",
                             requestBuildId.ShortDebugString().c_str());
    }
    auto principalGenerations = findPrincipalGeneration(keepers, requestBuildId);

    if (principalGenerations.size() != 1) {
        SET_ERROR_AND_RETURN(requestBuildId, response, ADMIN_BAD_REQUEST, "find [%lu] generation(s) with id[%d], not 1",
                             principalGenerations.size(), requestBuildId.generationid());
    }

    auto [principalGeneration, principalBuildId] = principalGenerations[0];

    std::set<indexlibv2::framework::VersionCoord> reservedVersions;
    if (!principalGeneration->getReservedVersions(request->clustername(), request->range(), &reservedVersions)) {
        SET_ERROR_AND_RETURN(request->buildid(), response, ADMIN_BAD_REQUEST,
                             "request [%s] get reserved versions failed",
                             request->buildid().ShortDebugString().c_str());
    }
    proto::StartBuildRequest startBuildRequest;
    startBuildRequest.set_configpath(principalGeneration->getConfigPath());
    *startBuildRequest.mutable_buildid() = principalBuildId;
    startBuildRequest.mutable_buildid()->set_appname(request->buildid().appname());
    startBuildRequest.set_buildmode("general_task");
    proto::StartBuildResponse startBuildResponse;
    startBuild(&startBuildRequest, &startBuildResponse);
    if (startBuildResponse.has_errorcode() && startBuildResponse.errorcode() != proto::ADMIN_ERROR_NONE) {
        SET_ERROR_AND_RETURN(request->buildid(), response, startBuildResponse.errorcode(),
                             "start general task failed, start build failed: %s",
                             startBuildResponse.ShortDebugString().c_str());
    }
    proto::StartTaskRequest startTaskRequest;
    *startTaskRequest.mutable_buildid() = startBuildRequest.buildid();
    startTaskRequest.set_taskid(request->taskid());
    startTaskRequest.set_taskname(BS_GENERAL_TASK);
    startTaskRequest.set_clustername(request->clustername());
    GeneralTaskParam param;
    param.branchId = request->branchid();
    param.taskEpochId = request->taskepochid();
    param.partitionIndexRoot = request->partitionindexroot();
    param.generationId = request->buildid().generationid();
    param.sourceVersionIds.push_back(request->sourceversionid()); // ? deprecate vector
    if (request->paramkeys_size() != request->paramvalues_size()) {
        SET_ERROR_AND_RETURN(request->buildid(), response, ADMIN_BAD_REQUEST,
                             "param keys & values mismatch, request[%s]", request->ShortDebugString().c_str());
    }
    param.plan.set(request->plan());
    for (size_t i = 0; i < request->paramkeys_size(); ++i) {
        param.params[request->paramkeys(i)] = request->paramvalues(i);
    }
    std::set<versionid_t> legacyReservedVersions;
    for (const auto& reservedVersion : reservedVersions) {
        legacyReservedVersions.emplace(reservedVersion.GetVersionId());
    }
    param.params[indexlibv2::table::RESERVED_VERSION_SET] = autil::legacy::ToJsonString(legacyReservedVersions);
    param.params[indexlibv2::table::RESERVED_VERSION_COORD_SET] = autil::legacy::ToJsonString(reservedVersions);
    std::string paramContent;
    try {
        paramContent = autil::legacy::ToJsonString(param);
    } catch (const autil::legacy::ExceptionBase& e) {
        SET_ERROR_AND_RETURN(request->buildid(), response, ADMIN_BAD_REQUEST, "serialize general task param failed: %s",
                             e.what());
    }
    startTaskRequest.set_taskparam(std::move(paramContent));
    startTask(&startTaskRequest, response);
}

void ServiceKeeper::fillWorkerNodeStatus(const proto::WorkerNodeStatusRequest* request,
                                         proto::WorkerNodeStatusResponse* response)
{
    const BuildId& buildId = request->buildid();
    bool isRecoverFailed = false;
    GenerationKeeperPtr generationKeeper = getGeneration(buildId, /*includeHistory=*/false, isRecoverFailed);
    if (isRecoverFailed) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                             "fillWorkerNodeStatus failed : [%s] exists, but recover failed.",
                             buildId.ShortDebugString().c_str());
    }
    if (!generationKeeper) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_NOT_FOUND, "fillWorkerNodeStatus failed : [%s] not exists.",
                             buildId.ShortDebugString().c_str());
    }

    std::set<std::string> roleNames;
    for (size_t i = 0; i < request->pids_size(); i++) {
        const PartitionId& pid = request->pids(i);
        roleNames.insert(RoleNameGenerator::generateRoleName(pid));
    }
    for (size_t i = 0; i < request->rolenames_size(); i++) {
        roleNames.insert(request->rolenames(i));
    }
    if (roleNames.empty()) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_BAD_REQUEST,
                             "fillWorkerNodeStatus for buildId [%s] failed : no target roles in request.",
                             buildId.ShortDebugString().c_str());
    }
    generationKeeper->fillWorkerNodeStatus(roleNames, response);
}

void ServiceKeeper::migrateTargetRoles(const proto::MigrateRoleRequest* request, proto::InformResponse* response)
{
    BS_LOG(INFO, "Begin migrateTargetRoles [%s].", request->ShortDebugString().c_str());
    string msg = "Begin migrateTargetRoles [" + request->ShortDebugString() + "].";
    beeper::EventTags collectTags = BuildIdWrapper::getEventTags(request->buildid());
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);

    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);

    const BuildId& buildId = request->buildid();
    bool isRecoverFailed = false;
    GenerationKeeperPtr generationKeeper = getGeneration(buildId, /*includeHistory=*/false, isRecoverFailed);
    if (isRecoverFailed) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR,
                             "migrateTargetRoles failed : [%s] exists, but recover failed.",
                             buildId.ShortDebugString().c_str());
    }
    if (!generationKeeper) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_NOT_FOUND, "migrateTargetRoles failed : [%s] not exists.",
                             buildId.ShortDebugString().c_str());
    }

    std::set<std::string> roleNames;
    for (size_t i = 0; i < request->pids_size(); i++) {
        const PartitionId& pid = request->pids(i);
        roleNames.insert(RoleNameGenerator::generateRoleName(pid));
    }
    for (size_t i = 0; i < request->rolenames_size(); i++) {
        roleNames.insert(request->rolenames(i));
    }
    if (roleNames.empty()) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_BAD_REQUEST,
                             "migrateTargetRoles for buildId [%s] failed : no target roles in request.",
                             buildId.ShortDebugString().c_str());
    }

    std::map<std::string, std::string> successRoleInfos;
    generationKeeper->migrateTargetRoles(roleNames, &successRoleInfos);

    if (successRoleInfos.size() == roleNames.size()) {
        response->set_errorcode(ADMIN_ERROR_NONE);
    } else {
        response->set_errorcode(ADMIN_INTERNAL_ERROR);
        for (auto [roleName, _] : successRoleInfos) {
            roleNames.erase(roleName);
        }
        std::vector<std::string> failedRoleNames(roleNames.begin(), roleNames.end());
        std::string errorMsg = "non-existent roles:";
        errorMsg += autil::legacy::ToJsonString(failedRoleNames, true);
        response->add_errormessage(errorMsg.c_str());
    }
    response->set_response(autil::legacy::ToJsonString(successRoleInfos, true));

    BS_LOG(INFO, "Finish migrateTargetRoles [%s], success: %s.", request->ShortDebugString().c_str(),
           autil::legacy::ToJsonString(successRoleInfos, true).c_str());
    msg = "Finish migrateTargetRoles [" + request->ShortDebugString() +
          "], success:" + autil::legacy::ToJsonString(successRoleInfos, true) + ".";
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);
}

size_t ServiceKeeper::getThreadNumFromEnv() const
{
    string param = autil::EnvUtil::getEnv(BS_ENV_RECOVER_THREAD_NUM.c_str());
    size_t threadNum = DEFAULT_THREAD_NUM;
    if (param.empty() || !StringUtil::fromString(param, threadNum)) {
        return DEFAULT_THREAD_NUM;
    }
    return threadNum;
}

size_t ServiceKeeper::getRecoverSleepSecondFromEnv() const
{
    string param = autil::EnvUtil::getEnv(BS_ENV_RECOVER_SLEEP_SECOND.c_str());
    size_t sleepSec = DEFAULT_RECOVER_SLEEP_SEC;
    if (param.empty() || !StringUtil::fromString(string(param), sleepSec)) {
        return DEFAULT_RECOVER_SLEEP_SEC;
    }
    return sleepSec;
}

bool ServiceKeeper::collectRecoverBuildIds(vector<BuildId>& buildIds)
{
    assert(_activeGenerationKeepers.empty());
    assert(_allGenerationKeepers.empty());
    string adminZkRoot = PathDefine::getAdminZkRoot(_zkRoot);
    bool dirExist = false;
    if (!fslib::util::FileUtil::isDir(adminZkRoot, dirExist)) {
        BS_LOG(ERROR, "check whether %s is dir failed.", adminZkRoot.c_str());
        return false;
    }
    if (!dirExist) {
        if (fslib::util::FileUtil::mkDir(adminZkRoot)) {
            return true;
        } else {
            string errorMsg = "mkDir [" + adminZkRoot + "] failed!";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
    }
    BS_LOG(INFO, "service keeper recover begin");
    if (!PathDefine::getAllGenerations(_zkRoot, buildIds)) {
        string errorMsg = "recover generation info from[" + _zkRoot + "] failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

void ServiceKeeper::waitRecoverFinished(const vector<BuildId>& buildIds,
                                        const OutputOrderedThreadPoolPtr& recoverThreadPool)
{
    for (size_t i = 0; i < buildIds.size(); i++) {
        recoverThreadPool->pushWorkItem(createGenerationRecoverWorkItem(buildIds[i]));
    }

    size_t waitInterval = 3000000;
    size_t lastWaitItemCount = numeric_limits<size_t>::max();
    while (true) {
        size_t waitItemCount = recoverThreadPool->getWaitItemCount();
        if (waitItemCount <= 0) {
            break;
        }

        string msg = "recover progress " + StringUtil::toString(buildIds.size() - waitItemCount) + "/" +
                     StringUtil::toString(buildIds.size());
        BEEPER_INTERVAL_REPORT(60, ADMIN_STATUS_COLLECTOR_NAME, msg);

        BS_LOG(INFO, "recover progress %ld/%ld", buildIds.size() - waitItemCount, buildIds.size());
        if (waitItemCount < lastWaitItemCount) {
            lastWaitItemCount = waitItemCount;
            _refreshTimestamp = TimeUtility::currentTimeInSeconds();
        }
        usleep(waitInterval);
    }
}

bool ServiceKeeper::recover(const vector<BuildId>& buildIds)
{
    GenerationKeepers activeGenerationKeepers;
    GenerationKeepers allGenerationKeepers;
    size_t threadNum = getThreadNumFromEnv();
    BS_LOG(INFO, "recover thread num: %ld", threadNum);
    string msg = "recover thread num: " + StringUtil::toString(threadNum);
    BEEPER_REPORT(ADMIN_STATUS_COLLECTOR_NAME, msg);

    OutputOrderedThreadPoolPtr recoverThreadPool(new OutputOrderedThreadPool(threadNum, buildIds.size()));
    recoverThreadPool->start("BsRecover");
    waitRecoverFinished(buildIds, recoverThreadPool);

    for (size_t i = 0; i < buildIds.size(); i++) {
        GenerationRecoverWorkItem* workItem = static_cast<GenerationRecoverWorkItem*>(recoverThreadPool->popWorkItem());
        GenerationRecoverWorkItem::RecoverStatus status = workItem->getRecoverStatus();
        if (status == GenerationRecoverWorkItem::RECOVER_SUCCESS) {
            GenerationKeeperPtr keeper = workItem->getGenerationKeeper();
            allGenerationKeepers[buildIds[i]] = keeper;
            if (keeper->isStopped()) {
                delete workItem;
                continue;
            }
            activeGenerationKeepers[buildIds[i]] = keeper;
            if (_metricsReporter) {
                GenerationMetricsReporterPtr generationMetricReporter =
                    _metricsReporter->createGenerationMetrics(buildIds[i]);
                keeper->resetMetricsReporter(generationMetricReporter);
            }

            string msg = "recover active generation [" + buildIds[i].ShortDebugString() + "] success.";
            BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, msg, BuildIdWrapper::getEventTags(buildIds[i]));
            delete workItem;
            continue;
        }
        if (status == GenerationRecoverWorkItem::RECOVER_FAILED) {
            _recoverFailedGenerations.insert(buildIds[i]);
            string msg = "recover generation [" + buildIds[i].ShortDebugString() + "] fail.";
            BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, msg, BuildIdWrapper::getEventTags(buildIds[i]));
            delete workItem;
            continue;
        }
        assert(status == GenerationRecoverWorkItem::NO_NEED_RECOVER);
        delete workItem;
    }
    {
        autil::ScopedLock lock(_mapLock);
        _activeGenerationKeepers = activeGenerationKeepers;
        if (_healthChecker) {
            for (GenerationKeepers::iterator it = activeGenerationKeepers.begin(); it != activeGenerationKeepers.end();
                 it++) {
                _healthChecker->addGenerationKeeper(it->first, it->second);
            }
        }
        _allGenerationKeepers = allGenerationKeepers;
    }

    BEEPER_REPORT(ADMIN_STATUS_COLLECTOR_NAME, "service keeper recover end");
    BS_LOG(INFO, "service keeper recover end");
    return _recoverFailedGenerations.empty();
}

void ServiceKeeper::syncAdminInfo()
{
    GenerationKeepers keepers;
    {
        ScopedLock lock(_mapLock);
        keepers = _activeGenerationKeepers;
    }
    bool syncCounter = CounterSynchronizer::needSyncCounter();
    for (GenerationKeepers::iterator it = keepers.begin(); it != keepers.end(); it++) {
        if (syncCounter) {
            it->second->syncCounters();
        }

        it->second->reportMetrics();
        if (it->second->isStopped()) {
            it->second->stealMetricsReporter();
        }
    }
}

void ServiceKeeper::keepServiceLoop()
{
    int64_t beginTs = TimeUtility::currentTime() / 1000;
    _refreshTimestamp = TimeUtility::currentTimeInSeconds();
    _scheduleTimestamp = _refreshTimestamp;
    if (_zkStatus && (_scheduleTimestamp - _lastSyncScheduleTs > DEFAULT_SYNC_SCHEDULE_TIMESTAMP_SEC)) {
        if (_zkStatus->write(StringUtil::toString(_scheduleTimestamp)) == WorkerState::EC_FAIL) {
            BS_LOG(ERROR, "update %s file fail!", PathDefine::getAdminScheduleTimestampFilePath(_zkRoot).c_str());
        }
        _lastSyncScheduleTs = _scheduleTimestamp;
    }

    resetMetricsRecord();
    releaseFailedSlots();

    RoleSlotInfos roleSlots;
    _scheduler->getAllRoleSlots(roleSlots);

    std::shared_ptr<RoleSlotInfos> roleSlotsPtr(new RoleSlotInfos);
    roleSlotsPtr->swap(roleSlots);

    RoleSlotInfos slowWorkerSlots;
    GenerationKeepers activeGenerationKeepers;
    {
        autil::ScopedLock lock(_mapLock);
        for (GenerationKeepers::iterator it = _activeGenerationKeepers.begin(); it != _activeGenerationKeepers.end();) {
            auto& currentKeeper = it->second;
            currentKeeper->updateSlotInfos(roleSlotsPtr);
            if (currentKeeper->isStopped()) {
                const BuildId& buildId = it->first;
                if (_indexInfoCollector) {
                    _indexInfoCollector->stop(it->first);
                }
                BS_LOG(INFO, "erase generation [%s]", buildId.ShortDebugString().c_str());
                currentKeeper->clearThreads();
                _activeGenerationKeepers.erase(it++);
            } else {
                if (_indexInfoCollector) {
                    _indexInfoCollector->update(it->first, currentKeeper);
                }

                currentKeeper->fillSlowWorkerSlots(slowWorkerSlots);
                GenerationKeeper::GenerationMetricsPtr generationMetrics = currentKeeper->getGenerationMetrics();
                if (generationMetrics) {
                    _globalMetricRecord.activeNodeCount += generationMetrics->activeNodeCount;
                    _globalMetricRecord.assignSlotCount += generationMetrics->assignSlotCount;
                }

                _globalMetricRecord.startingGenerationCount += currentKeeper->isStarting() ? 1 : 0;
                _globalMetricRecord.stoppingGenerationCount += currentKeeper->isStopping() ? 1 : 0;

                proto::BuildStep buildStep;
                if (currentKeeper->getBuildStep(buildStep)) {
                    _globalMetricRecord.fullBuildingGenerationCount += (buildStep == BUILD_STEP_FULL) ? 1 : 0;
                    _globalMetricRecord.incBuildingGenerationCount += (buildStep == BUILD_STEP_INC) ? 1 : 0;
                }
                ++it;
            }
        }
        activeGenerationKeepers = _activeGenerationKeepers;
        _globalMetricRecord.allGenerationCount = _allGenerationKeepers.size();
        _globalMetricRecord.activeGenerationCount = _activeGenerationKeepers.size();
        _globalMetricRecord.recoverFailedGenerationCount = _recoverFailedGenerations.size();
    }
    releaseSlowWorkerSlots(slowWorkerSlots);

    _globalMetricRecord.waitSlotNodeCount = _globalMetricRecord.activeNodeCount - _globalMetricRecord.assignSlotCount;
    vector<std::string> prohibitedIps;
    if (_prohibitedIpsTable) {
        _prohibitedIpsTable->getProhibitedIps(prohibitedIps);
        BS_LOG(DEBUG, "prohibit ips: [%s]", StringUtil::toString(prohibitedIps, ",").c_str());
    }

    AppPlan appPlan = generateAppPlan(activeGenerationKeepers, prohibitedIps);
    _scheduler->setAppPlan(appPlan);

    int64_t endTs = TimeUtility::currentTime() / 1000;
    _globalMetricRecord.keepServiceLoopInterval = endTs - beginTs;
    if (_healthChecker) {
        _healthChecker->reportScheduleFreshness((endTs - beginTs) / 1000);
    }

    if (_globalMetricRecord.keepServiceLoopInterval > 1000) {
        string msg = "keepServiceLoop cost too much time [" +
                     StringUtil::toString(_globalMetricRecord.keepServiceLoopInterval) + "] ms";
        BEEPER_INTERVAL_REPORT(30, ADMIN_STATUS_COLLECTOR_NAME, msg);
    }

    _globalMetricRecord.prohibitedIpCount = prohibitedIps.size();
    if (_metricsReporter) {
        _metricsReporter->reportMetrics(_globalMetricRecord);
    }
}

void ServiceKeeper::releaseSlowWorkerSlots(const RoleSlotInfos& slowWorkerSlots)
{
    // here include two kind of slots : reclaiming slot and slow worker slot
    // (1) not all slow worker will be released here if enable backup strategy of slow node detect
    AgentSimpleMasterScheduler* agentScheduler = dynamic_cast<AgentSimpleMasterScheduler*>(_scheduler);
    vector<hippo::SlotId> toReleaseSlots;
    for (const auto& it : slowWorkerSlots) {
        const string& roleName = it.first;
        const SlotInfos& slotInfos = it.second;
        if (agentScheduler && agentScheduler->isUsingAgentNode(roleName)) {
            auto blackAgent = agentScheduler->addTargetRoleToBlackList(roleName, slotInfos);
            if (blackAgent.empty()) {
                continue;
            }
            BS_LOG(INFO, "SLOW worker detected: [%s] in agent [%s] will be re-scheduled", roleName.c_str(),
                   blackAgent.c_str());
            if (agentScheduler->hasValidTargetRoles(blackAgent)) {
                continue;
            }
            // agent no valid target roles will be released by slot info
            BS_LOG(INFO, "SLOW agent node [%s] will be released for no valid target roles", blackAgent.c_str());
        }
        vector<string> slotAddrs;
        slotAddrs.reserve(slotInfos.size());
        for (const auto& slotInfo : slotInfos) {
            toReleaseSlots.push_back(slotInfo.slotId);
            slotAddrs.push_back(slotInfo.slotId.slaveAddress);
        }
        BS_LOG(INFO, "SLOW worker detected: [%s], addr [%s], will be released soon", roleName.c_str(),
               StringUtil::toString(slotAddrs, ";").c_str());
    }

    if (!toReleaseSlots.empty()) {
        _globalMetricRecord.slowSlotCount = toReleaseSlots.size();
        hippo::ReleasePreference pref;
        pref.type = hippo::ReleasePreference::RELEASE_PREF_PROHIBIT;
        pref.leaseMs = 10 * 60 * 1000; // 10 min
        _scheduler->releaseSlots(toReleaseSlots, pref);
    }
}

void ServiceKeeper::releaseFailedSlots()
{
    AgentSimpleMasterScheduler* agentScheduler = dynamic_cast<AgentSimpleMasterScheduler*>(_scheduler);
    map<string, SlotInfos> roleSlots;
    _scheduler->getAllRoleSlots(roleSlots);

    vector<hippo::SlotId> toReleaseSlots;
    for (const auto& it : roleSlots) {
        const string& roleName = it.first;
        const SlotInfos& slotInfos = it.second;
        if (slotInfos.size() > 1) {
            BS_LOG(WARN, "%s has more than 1 replica", roleName.c_str());
        }
        for (size_t i = 0; i < slotInfos.size(); i++) {
            const hippo::SlotInfo& oneSlot = slotInfos[i];
            bool failed = false;
            for (size_t j = 0; j < oneSlot.processStatus.size(); j++) {
                if (oneSlot.processStatus[j].status == hippo::ProcessStatus::PS_FAILED) {
                    BS_LOG(WARN, "slot[%s] has fatal error for role[%s], will release soon",
                           ToJsonString(oneSlot.slotId).c_str(), roleName.c_str());
                    failed = true;
                    break;
                }
            }
            if (failed) {
                if (agentScheduler && agentScheduler->isUsingAgentNode(roleName)) {
                    auto blackAgent = agentScheduler->addTargetRoleToBlackList(roleName, slotInfos);
                    if (blackAgent.empty()) {
                        continue;
                    }
                    BS_LOG(INFO, "failed slot detected: [%s] in agent [%s] will be re-scheduled", roleName.c_str(),
                           blackAgent.c_str());
                    if (agentScheduler->hasValidTargetRoles(blackAgent)) {
                        continue;
                    }
                    // agent no valid target roles will be released by slot info
                    BS_LOG(INFO, "FAILED agent node [%s] will be released for no valid target roles",
                           blackAgent.c_str());
                }
                toReleaseSlots.push_back(oneSlot.slotId);
            }
        }
    }

    if (!toReleaseSlots.empty()) {
        _globalMetricRecord.failSlotCount = toReleaseSlots.size();
        hippo::ReleasePreference pref;
        pref.type = hippo::ReleasePreference::RELEASE_PREF_PROHIBIT;
        pref.leaseMs = 10 * 60 * 1000; // 10 min

        _scheduler->releaseSlots(toReleaseSlots, pref);
    }
}

int32_t ServiceKeeper::getSyncCounterInterval() const
{
    string param = autil::EnvUtil::getEnv(BS_ENV_SYNC_COUNTER_INTERVAL.c_str());
    int32_t defaultSyncInterval = 1;
    int32_t syncInteravl = defaultSyncInterval;
    if (!param.empty() && StringUtil::fromString(param, syncInteravl) && syncInteravl > 0) {
        return syncInteravl;
    }
    return defaultSyncInterval;
}

bool ServiceKeeper::needFillCounterInfo(const ServiceInfoRequest* request) const
{
    if (!CounterSynchronizer::needSyncCounter()) {
        return false;
    }

    if (!request->has_buildid()) {
        return false;
    }

    const BuildId buildId = request->buildid();
    return buildId.has_appname() && buildId.has_datatable() && buildId.has_generationid();
}

bool ServiceKeeper::isBuildIdMatchRequest(const ServiceInfoRequest* request, const BuildId& buildId) const
{
    if (request->has_jobid()) {
        return isMatchedBuildId(request->jobid(), buildId);
    }
    if (request->has_appname() && buildId.appname() != request->appname()) {
        return false;
    }
    if (request->has_buildid()) {
        const BuildId buildIdFilter = request->buildid();
        if (buildIdFilter.has_appname() && buildIdFilter.appname() != buildId.appname()) {
            return false;
        }
        if (buildIdFilter.has_datatable() && buildIdFilter.datatable() != buildId.datatable()) {
            return false;
        }
        if (buildIdFilter.has_generationid() && buildIdFilter.generationid() != buildId.generationid()) {
            return false;
        }
    }

    return true;
}

bool ServiceKeeper::isBuildIdMatched(const BuildId& requestBuildId, const BuildId& candidateBuildId) const
{
    if (requestBuildId.has_appname() && requestBuildId.appname() != candidateBuildId.appname()) {
        return false;
    }
    if (requestBuildId.has_datatable() && requestBuildId.datatable() != candidateBuildId.datatable()) {
        return false;
    }
    return requestBuildId.has_generationid() && requestBuildId.generationid() == candidateBuildId.generationid();
}

string ServiceKeeper::buildIdToJobId(const proto::BuildId& buildId)
{
    stringstream ss;
    ss << buildId.appname() << "." << buildId.datatable() << "." << buildId.generationid();
    return ss.str();
}

bool ServiceKeeper::isMatchedBuildId(const string& jobId, const BuildId& buildId) const
{
    string tmpJobId = buildIdToJobId(buildId);
    return jobId == tmpJobId;
}

void ServiceKeeper::fillServiceInfo(const proto::ServiceInfoRequest* request,
                                    proto::ServiceInfoResponse* response) const
{
    int64_t beginTime = autil::TimeUtility::currentTime();
    REPORT_KMONITOR_METRIC(_keeperMetric.kgsQps, 1);

    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);

    GenerationKeepers keepers;
    set<BuildId> recoverFailedGenerations;
    {
        ScopedLock lock(_mapLock);
        if (request->gethistory()) {
            keepers = _allGenerationKeepers;
        } else {
            keepers = _activeGenerationKeepers;
        }
        recoverFailedGenerations = _recoverFailedGenerations;
    }
    bool ignoreTemplate = request->ignoretemplate();
    bool fillCounterInfo = needFillCounterInfo(request);

    for (GenerationKeepers::const_iterator it = keepers.begin(); it != keepers.end(); ++it) {
        if (isBuildIdMatchRequest(request, it->first)) {
            const GenerationKeeperPtr& keeper = it->second;
            GenerationInfo* generationInfo = response->add_generationinfos();
            if (!ignoreTemplate) {
                GenerationInfo tmpGenerationInfo;
                *(tmpGenerationInfo.mutable_buildid()) = it->first;
                keeper->fillGenerationInfo(&tmpGenerationInfo, request->filltaskhistory(), fillCounterInfo,
                                           request->fillflowinfo(), request->filleffectiveindexinfo());

                _serviceInfoFilter.filter(tmpGenerationInfo, *generationInfo);
            } else {
                *(generationInfo->mutable_buildid()) = it->first;
                keeper->fillGenerationInfo(generationInfo, request->filltaskhistory(), fillCounterInfo,
                                           request->fillflowinfo(), request->filleffectiveindexinfo());
            }
        }
    }
    for (auto& it : recoverFailedGenerations) {
        if (isBuildIdMatchRequest(request, it)) {
            GenerationInfo* generationInfo = response->add_generationinfos();
            *generationInfo->mutable_buildid() = it;
            generationInfo->set_hasfatalerror(true);
            *generationInfo->mutable_generationfatalerrormsg() = it.ShortDebugString() + " recover failed";
        }
    }

    if (request->fillglobalagentinfo()) {
        auto agentMaintainer = getGlobalAgentMaintainer();
        assert(agentMaintainer);
        auto agentRolePlan = agentMaintainer->getAgentRoleTargetMap();
        if (agentRolePlan) {
            for (auto& item : *agentRolePlan) {
                auto agentNodePlan = response->mutable_globalagentnodeplans()->Add();
                agentNodePlan->set_agentrolename(item.first);
                agentNodePlan->set_agentidentifier(item.second.first);
                for (const auto& roleName : item.second.second) {
                    agentNodePlan->add_targetrolename(roleName);
                }
            }
        }
    }

    int64_t endTime = autil::TimeUtility::currentTime();
    double latency = (endTime - beginTime) / 1000.0;
    REPORT_KMONITOR_METRIC(_keeperMetric.kgsLatency, latency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestLatency, latency);
}

void ServiceKeeper::cleanVersions(const proto::CleanVersionsRequest* request, proto::InformResponse* response)
{
    kmonitor_adapter::ScopeLatencyReporter kreporter(_keeperMetric.krpcRequestLatency);
    REPORT_KMONITOR_METRIC(_keeperMetric.krpcRequestQps, 1);

    string msg = "Begin cleanVersions task[" + request->ShortDebugString() + "].";
    beeper::EventTags collectTags = BuildIdWrapper::getEventTags(request->buildid());
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);

    const BuildId& buildId = request->buildid();
    const string& clusterName = request->clustername();
    versionid_t version = request->versionid();
    if (!_cleaner->cleanVersions(buildId, clusterName, version)) {
        SET_ERROR_AND_RETURN(buildId, response, ADMIN_INTERNAL_ERROR, "clean versions [%s] failed",
                             request->ShortDebugString().c_str());
    }

    msg = "Finish cleanVersions task[" + request->ShortDebugString() + "].";
    BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, msg, collectTags);
}

bool ServiceKeeper::getActiveGenerationsToStop(const proto::BuildId& buildId,
                                               vector<GenerationKeeperPtr>& generations) const
{
    bool needCompareDataTable = buildId.has_datatable();
    bool needCompareGid = buildId.has_generationid();

    autil::ScopedLock lock(_mapLock);
    set<BuildId>::const_iterator ite = _recoverFailedGenerations.begin();
    for (; ite != _recoverFailedGenerations.end(); ite++) {
        const BuildId& bid = *ite;
        if (bid.appname() != buildId.appname()) {
            continue;
        }
        if (needCompareDataTable && bid.datatable() != buildId.datatable()) {
            continue;
        }
        if (needCompareDataTable && needCompareGid && bid.generationid() != buildId.generationid()) {
            continue;
        }
        return false;
    }
    GenerationKeepers::const_iterator it = _activeGenerationKeepers.begin();
    for (; it != _activeGenerationKeepers.end(); it++) {
        const BuildId& bid = it->first;
        if (bid.appname() != buildId.appname()) {
            continue;
        }
        if (needCompareDataTable && bid.datatable() != buildId.datatable()) {
            continue;
        }
        if (needCompareDataTable && needCompareGid && bid.generationid() != buildId.generationid()) {
            continue;
        }
        generations.push_back(it->second);
        if (needCompareGid) {
            // gid is unique
            break;
        }
    }
    return true;
}

void ServiceKeeper::removeStoppedGeneration(const proto::BuildId& buildId)
{
    autil::ScopedLock lock(_mapLock);
    GenerationKeepers::iterator it = _activeGenerationKeepers.find(buildId);
    if (_activeGenerationKeepers.end() == it) {
        _allGenerationKeepers.erase(buildId);
    }
}

const GenerationKeeperPtr& ServiceKeeper::getGeneration(const proto::BuildId& buildId, bool includeHistory,
                                                        bool& isRecoverFailed, bool fuzzy)
{
    autil::ScopedLock lock(_mapLock);
    isRecoverFailed = false;
    if (_recoverFailedGenerations.find(buildId) != _recoverFailedGenerations.end()) {
        isRecoverFailed = true;
        static GenerationKeeperPtr empty;
        return empty;
    }
    if (!includeHistory) {
        return findGeneration(_activeGenerationKeepers, buildId, fuzzy);
    } else {
        return findGeneration(_allGenerationKeepers, buildId, fuzzy);
    }
}

const GenerationKeeperPtr& ServiceKeeper::findGeneration(const GenerationKeepers& keepers,
                                                         const proto::BuildId& buildId, bool fuzzy) const
{
    static GenerationKeeperPtr empty;
    if (!fuzzy) {
        auto iter = keepers.find(buildId);
        if (iter != keepers.end()) {
            return iter->second;
        }
        return empty;
    }

    auto targetIter = keepers.end();
    for (auto iter = keepers.begin(); iter != keepers.end(); ++iter) {
        if (isBuildIdMatched(buildId, iter->first)) {
            if (targetIter != keepers.end()) {
                BS_LOG(WARN, "multi generation mathched: [%s][%s]",
                       targetIter->second->getBuildId().ShortDebugString().c_str(),
                       iter->first.ShortDebugString().c_str());
                return empty;
            }
            targetIter = iter;
        }
    }
    if (targetIter != keepers.end()) {
        return targetIter->second;
    }
    return empty;
}

std::vector<GenerationKeeperPtr> ServiceKeeper::getAttachedGenerations(const proto::BuildId& buildId) const
{
    if (!buildId.has_generationid()) {
        return {};
    }
    std::vector<GenerationKeeperPtr> ret;
    for (const auto& [id, keeper] : _activeGenerationKeepers) {
        if (keeper->getGenerationTaskType() != GenerationTaskBase::TT_GENERAL) {
            continue;
        }
        if (buildId.has_datatable() && buildId.datatable() != id.datatable()) {
            continue;
        }
        if (buildId.generationid() != id.generationid()) {
            continue;
        }
        auto appName = ProtoUtil::getOriginalAppName(id.appname());
        if (!appName.empty() && appName != buildId.appname()) {
            continue;
        }
        ret.push_back(keeper);
    }
    return ret;
}

void ServiceKeeper::setGeneration(const proto::BuildId& buildId, const GenerationKeeperPtr& keeper)
{
    autil::ScopedLock lock(_mapLock);
    assert(_activeGenerationKeepers.find(buildId) == _activeGenerationKeepers.end());

    if (_metricsReporter) {
        GenerationMetricsReporterPtr generationMetricReporter = _metricsReporter->createGenerationMetrics(buildId);
        keeper->resetMetricsReporter(generationMetricReporter);
    }
    _activeGenerationKeepers[buildId] = keeper;
    _allGenerationKeepers[buildId] = keeper;
}

void ServiceKeeper::getStoppedGenerations(BizGenerationsMap& generations) const
{
    autil::ScopedLock lock(_mapLock);
    for (auto it = _allGenerationKeepers.begin(); it != _allGenerationKeepers.end(); ++it) {
        BuildId buildId = it->first;
        if (_activeGenerationKeepers.find(buildId) != _activeGenerationKeepers.end()) {
            continue;
        }
        generations[make_pair(buildId.appname(), buildId.datatable())].push_back(it->second);
    }
}

AppPlan ServiceKeeper::generateAppPlan(const GenerationKeepers& generationKeepers,
                                       const std::vector<std::string>& prohibitedIps)
{
    AgentSimpleMasterScheduler* agentScheduler = dynamic_cast<AgentSimpleMasterScheduler*>(_scheduler);
    BS_LOG(DEBUG, "[test] generateAppPlan begin");

    auto agentMaintainer = getGlobalAgentMaintainer();
    assert(agentMaintainer);

    std::set<proto::BuildId> agentInUseBuildIds;
    AppPlan appPlan;
    appPlan.prohibitedIps = prohibitedIps;
    for (GenerationKeepers::const_iterator it = generationKeepers.begin(); it != generationKeepers.end(); ++it) {
        auto generationRolePlan = it->second->generateRolePlan(agentScheduler);
        if (!generationRolePlan || !generationRolePlan->appPlan) {
            BS_LOG(ERROR, "Generation[%s] return NULL RolePlan", it->first.ShortDebugString().c_str());
            continue;
        }
        appPlan.rolePlans.insert(generationRolePlan->appPlan->rolePlans.begin(),
                                 generationRolePlan->appPlan->rolePlans.end());
        if (!agentScheduler) {
            continue;
        }
        if (generationRolePlan->agentRolePlan != nullptr && !generationRolePlan->agentRolePlan->empty()) {
            // generation level agent nodes
            agentInUseBuildIds.insert(it->first);
            set<string> currentAgentRoles;
            agentScheduler->getAgentRoleNames(it->first, currentAgentRoles);
            for (auto& [agentRole, agentRoleInfo] : *generationRolePlan->agentRolePlan) {
                if (!agentScheduler->declareAgentRole(agentRole, agentRoleInfo)) {
                    BS_LOG(ERROR, "declareAgentRole [%s] with fail.", agentRole.c_str());
                    continue;
                }
                currentAgentRoles.erase(agentRole);
            }
            if (!currentAgentRoles.empty()) {
                BS_LOG(INFO, "remove useless agent nodes for [%s]", it->first.ShortDebugString().c_str());
                for (auto& roleName : currentAgentRoles) {
                    agentScheduler->removeAgentRole(roleName);
                }
            }
        }
        if (generationRolePlan->allowAssignToGlobalAgents) {
            agentMaintainer->addTargetRolePlan(generationRolePlan->appPlan->rolePlans,
                                               generationRolePlan->agentRolePlan, generationRolePlan->configPath,
                                               generationRolePlan->targetGlobalAgentGroupName);
        }
    }
    if (agentScheduler) {
        // global level agent nodes
        auto globalAgentRolePlan = agentMaintainer->makePlan(agentScheduler, appPlan);
        if (globalAgentRolePlan && !globalAgentRolePlan->empty()) {
            std::set<proto::BuildId> agentBuildIds;
            agentMaintainer->fillGlobalAgentBuildIds(agentBuildIds);
            set<string> currentAgentRoles;
            for (auto& id : agentBuildIds) {
                agentInUseBuildIds.insert(id);
                agentScheduler->getAgentRoleNames(id, currentAgentRoles);
            }
            for (auto& [agentRole, agentRoleInfo] : *globalAgentRolePlan) {
                if (!agentScheduler->declareAgentRole(agentRole, agentRoleInfo)) {
                    BS_LOG(ERROR, "declareAgentRole [%s] with fail.", agentRole.c_str());
                    continue;
                }
                currentAgentRoles.erase(agentRole);
            }
            for (auto& roleName : currentAgentRoles) {
                agentScheduler->removeAgentRole(roleName);
            }
        }
        agentScheduler->removeUselessAgentNodes(agentInUseBuildIds);
    }

    if (!agentMaintainer->getFlexibleScaleConfigs().empty()) {
        map<pair<string, string>, uint32_t> updatePlan;
        agentMaintainer->getFlexibleUpdatePlan(updatePlan);
        if (!updateGlobalAgentNodeCount(agentMaintainer, updatePlan)) {
            BS_LOG(ERROR, "flexible update global agent node count fail.");
        }
    }
    BS_LOG(DEBUG, "[test] generateAppPlan end");
    return appPlan;
}

void ServiceKeeper::resetMetricsRecord() { _globalMetricRecord.reset(); }

GenerationRecoverWorkItem* ServiceKeeper::createGenerationRecoverWorkItem(const BuildId& buildId)
{
    GenerationKeeperPtr keeper = createGenerationKeeper(buildId);
    return new GenerationRecoverWorkItem(_zkRoot, buildId, keeper);
}

bool ServiceKeeper::needRecover()
{
    autil::ScopedLock lock(_mapLock);
    return !_recoverFailedGenerations.empty();
}
void ServiceKeeper::recoverLoop()
{
    size_t sleepSec = getRecoverSleepSecondFromEnv();
    while (needRecover()) {
        std::set<proto::BuildId> recoverFailedGenerations;
        GenerationKeepers generationKeepers;
        {
            autil::ScopedLock lock(_mapLock);
            recoverFailedGenerations = _recoverFailedGenerations;
        }

        string msg = "background recover failed generation count [" +
                     StringUtil::toString(recoverFailedGenerations.size()) + "]";
        BEEPER_INTERVAL_REPORT(300, ADMIN_STATUS_COLLECTOR_NAME, msg);

        std::set<proto::BuildId>::iterator ite = recoverFailedGenerations.begin();
        while (ite != recoverFailedGenerations.end()) {
            GenerationRecoverWorkItem* workItem = createGenerationRecoverWorkItem(*ite);
            workItem->process();
            GenerationRecoverWorkItem::RecoverStatus status = workItem->getRecoverStatus();
            if (status == GenerationRecoverWorkItem::RECOVER_FAILED) {
                delete workItem;
                ite++;
                continue;
            }

            if (status == GenerationRecoverWorkItem::RECOVER_SUCCESS) {
                GenerationKeeperPtr keeper = workItem->getGenerationKeeper();
                generationKeepers[*ite] = keeper;
                if (_metricsReporter) {
                    GenerationMetricsReporterPtr generationMetricReporter =
                        _metricsReporter->createGenerationMetrics(*ite);
                    keeper->resetMetricsReporter(generationMetricReporter);
                }
            }
            delete workItem;
            recoverFailedGenerations.erase(ite++);
        }
        {
            autil::ScopedLock lock(_mapLock);
            _recoverFailedGenerations = recoverFailedGenerations;
            for (auto it = generationKeepers.begin(); it != generationKeepers.end(); it++) {
                _allGenerationKeepers[it->first] = it->second;
                if (it->second->isStopped()) {
                    continue;
                }
                _activeGenerationKeepers[it->first] = it->second;
                string msg = "recover active generation [" + it->first.ShortDebugString() + "] success.";
                BEEPER_REPORT(GENERATION_STATUS_COLLECTOR_NAME, msg, BuildIdWrapper::getEventTags(it->first));
                if (_healthChecker) {
                    _healthChecker->addGenerationKeeper(it->first, it->second);
                }
            }
        }
        usleep(sleepSec * 1000 * 1000);
        BS_LOG(INFO, "recover thread sleep[%zu]", sleepSec);
        msg = "background recover thread sleep [" + StringUtil::toString(sleepSec) + "]s";
        BEEPER_INTERVAL_REPORT(300, ADMIN_STATUS_COLLECTOR_NAME, msg);
    }
    BS_LOG(INFO, "background recover all generation success");
    string msg = "background recover all generation success";
    BEEPER_INTERVAL_REPORT(300, ADMIN_STATUS_COLLECTOR_NAME, msg);
}

int64_t ServiceKeeper::getLastRefreshTime() const { return _refreshTimestamp; }

int64_t ServiceKeeper::getLastScheduleTimestamp() const { return _scheduleTimestamp; }

void ServiceKeeper::setGlobalAgentMaintainer(const std::shared_ptr<GlobalAgentMaintainer>& agentMaintainer)
{
    _globalAgentMaintainerGuard.set(agentMaintainer);
}

std::shared_ptr<GlobalAgentMaintainer> ServiceKeeper::getGlobalAgentMaintainer() const
{
    std::shared_ptr<GlobalAgentMaintainer> ret;
    _globalAgentMaintainerGuard.get(ret);
    return ret;
}

bool ServiceKeeper::doInit() { return true; }

std::string ServiceKeeper::getBuildStepString(const GenerationKeeperPtr& keeper) const
{
    string buildStepStr = "unknown";
    proto::BuildStep buildStep;
    if (keeper->getBuildStep(buildStep)) {
        switch (buildStep) {
        case BUILD_STEP_FULL:
            buildStepStr = "full";
            break;
        case BUILD_STEP_INC:
            buildStepStr = "inc";
            break;
        case NO_BUILD_STEP:
            buildStepStr = "no_build";
            break;
        case BUILD_STEP_IDLE:
            buildStepStr = "idle";
            break;
        default:
            buildStepStr = "unknown";
        }
    }
    return buildStepStr;
}

std::string ServiceKeeper::getGenerationStepString(const GenerationKeeperPtr& keeper) const
{
    string str = "unknown";
    auto step = keeper->getGenerationStep();
    switch (step) {
    case GenerationTaskBase::GENERATION_STARTING:
        str = "starting";
        break;
    case GenerationTaskBase::GENERATION_STARTED:
        str = "started";
        break;
    case GenerationTaskBase::GENERATION_STOPPING:
        str = "stopping";
        break;
    case GenerationTaskBase::GENERATION_STOPPED:
        str = "stopped";
        break;
    case GenerationTaskBase::GENERATION_IDLE:
        str = "idle";
        break;
    default:
        assert(step == GenerationTaskBase::GENERATION_UNKNOWN);
    }
    return str;
}
}} // namespace build_service::admin
