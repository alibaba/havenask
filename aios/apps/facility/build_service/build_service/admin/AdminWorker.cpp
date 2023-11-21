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
#include "build_service/admin/AdminWorker.h"

#include "autil/ClosureGuard.h"
#include "autil/EnvUtil.h"
#include "beeper/beeper.h"
#include "build_service/admin/AdminInfo.h"
#include "build_service/admin/AgentSimpleMasterScheduler.h"
#include "build_service/admin/SimpleMasterSchedulerLocal.h"
#include "build_service/admin/catalog/CatalogServiceKeeper.h"
#include "build_service/admin/controlflow/LocalLuaScriptReader.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/PathDefine.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/LeaderCheckerGuard.h"
#include "fslib/common/common_define.h"
#include "fslib/util/FileUtil.h"
#include "kmonitor_adapter/MonitorFactory.h"
#include "worker_framework/LeaderInfo.h"

using namespace std;
using namespace autil;

using namespace build_service::util;
using namespace build_service::config;
using namespace build_service::common;
using namespace build_service::proto;

USE_MASTER_FRAMEWORK_NAMESPACE(simple_master);

namespace worker_framework {
WorkerBase* createWorker() { return new build_service::admin::AdminWorker(); }
}; // namespace worker_framework

namespace build_service { namespace admin {

BS_LOG_SETUP(admin, AdminWorker);

#define RETURN_WITH_ERROR(msg)                                                                                         \
    do {                                                                                                               \
        string errorMessage = msg;                                                                                     \
        BS_LOG(WARN, "%s", errorMessage.c_str());                                                                      \
        response->add_errormessage(errorMessage);                                                                      \
        return;                                                                                                        \
    } while (0)

#define RPC_GUARD()                                                                                                    \
    autil::ClosureGuard closure(done);                                                                                 \
    do {                                                                                                               \
        if (!_isStarted) {                                                                                             \
            RETURN_WITH_ERROR("admin not in service");                                                                 \
        }                                                                                                              \
    } while (0)

const size_t AdminWorker::RPC_SERVICE_THREAD_NUM = 16;
const size_t AdminWorker::RPC_SERVICE_QUEUE_SIZE = 500;
const string AdminWorker::RPC_SERVICE_THEAD_POOL_NAME = "bs_admin_tp";

AdminWorker::AdminWorker() : _isStarted(false) {}

AdminWorker::~AdminWorker() { doStop(); }

void AdminWorker::doInitOptions()
{
    getOptionParser().addOption("", "--app_name", "app_name", OptionParser::OPT_STRING, true);
    getOptionParser().addOption("", "--hippo", "hippo_root", OptionParser::OPT_STRING, true);
    getOptionParser().addOption("", "--zookeeper", "zookeeper_root", OptionParser::OPT_STRING, true);
    getOptionParser().addOption("", "--amonitor_port", "amonitor_port", OptionParser::OPT_UINT32, true);
    getOptionParser().addOption("", "--prohibit_time", "prohibit_time", OptionParser::OPT_UINT32, false);
    getOptionParser().addOption("", "--prohibit_num", "prohibit_num", OptionParser::OPT_UINT32, false);
    getOptionParser().addOption("", "--beeper_config_path", "beeper_config_path", OptionParser::OPT_STRING, false);
    getOptionParser().addOption("", "--leader_lease_second", "leader_lease_second", OptionParser::OPT_INT32, false);
    getOptionParser().addOption("", "--leader_loop_interval_second", "leader_loop_interval_second",
                                OptionParser::OPT_INT32, false);
    getOptionParser().addOption("", "--local_mode", "local_mode", OptionParser::STORE_TRUE);
    getOptionParser().addOption("", "--catalog_address", "catalog_address", OptionParser::OPT_STRING, false);
    getOptionParser().addOption("", "--catalog_name", "catalog_name", OptionParser::OPT_STRING, false);
    getOptionParser().addMultiValueOption("", "--env", "env");
}

bool AdminWorker::doInit()
{
    string catalogAddress;
    string catalogName;
    getOptionParser().getOptionValue("catalog_address", catalogAddress);
    getOptionParser().getOptionValue("catalog_name", catalogName);
    if ((catalogAddress.empty() && !catalogName.empty()) || (!catalogAddress.empty() && catalogName.empty())) {
        BS_LOG(ERROR, "catalogAddress [%s] or catalogName [%s] is empty", catalogAddress.c_str(), catalogName.c_str());
        return false;
    }
    _keeper = catalogAddress.empty() ? new ServiceKeeper : new CatalogServiceKeeper(catalogAddress, catalogName);

    arpc::ThreadPoolDescriptor tpd;
    if (!registerService(this, tpd)) {
        BS_LOG(ERROR, "register service failed!");
        return false;
    }

    if (!initHTTPRPCServer()) {
        BS_LOG(ERROR, "register http service failed!");
        return false;
    }
    initEnvArgs();
    return true;
}

void AdminWorker::initEnvArgs()
{
    auto& optionParser = getOptionParser();
    vector<string> envArgs;
    if (!optionParser.getOptionValue("env", envArgs)) {
        return;
    }
    for (auto& envArg : envArgs) {
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

bool AdminWorker::startMonitor()
{
    uint32_t agentPort;
    if (!getOptionParser().getOptionValue("amonitor_port", agentPort)) {
        BS_LOG(WARN, "get amonitor port failed.");
        return false;
    }
    if (agentPort > numeric_limits<uint16_t>::max()) {
        BS_LOG(WARN, "invalid amonitor agent port[%d]", agentPort);
        return false;
    }

    string appName;
    if (!getOptionParser().getOptionValue("app_name", appName)) {
        BS_LOG(WARN, "get app name failed.");
        return false;
    }

    string kmonServiceName =
        autil::EnvUtil::getEnv(BS_ENV_ADMIN_KMON_SERVICE_NAME, BS_ENV_ADMIN_DEFAULT_KMON_SERVICE_NAME);
    BS_LOG(INFO, "set kmonservice name[%s]", kmonServiceName.c_str());
    _monitor = kmonitor_adapter::MonitorFactory::getInstance()->createMonitor(kmonServiceName);
    return true;
}

bool AdminWorker::doStart()
{
    initBeeper();
    BEEPER_REPORT(ADMIN_STATUS_COLLECTOR_NAME, "admin worker starting ...");
    BS_LOG(INFO, "admin worker starting ...");
    string zkRoot;
    getOptionParser().getOptionValue("zookeeper_root", zkRoot);
    string adminServiceName;
    getOptionParser().getOptionValue("app_name", adminServiceName);
    uint32_t amonitorPort;
    getOptionParser().getOptionValue("amonitor_port", amonitorPort);
    uint32_t prohibitTime = DEFAULT_PROHIBIT_TIME;
    getOptionParser().getOptionValue("prohibit_time", prohibitTime);
    uint32_t prohibitNum = DEFAULT_PROHIBIT_NUM;
    getOptionParser().getOptionValue("prohibit_num", prohibitNum);
    int32_t leaseInSecond = worker_framework::LeaderedWorkerBase::LEASE_IN_SECOND;
    getOptionParser().getOptionValue("leader_lease_second", leaseInSecond);
    int32_t leaderLoopIntervalInSecond = worker_framework::LeaderedWorkerBase::LOOP_INTERVAL;
    getOptionParser().getOptionValue("leader_loop_interval_second", leaderLoopIntervalInSecond);

    unique_ptr<LeaderCheckerGuard> lc(new LeaderCheckerGuard(getLeaderElector()));
    if (!initLeaderElector(common::PathDefine::getAdminZkRoot(zkRoot), leaseInSecond, leaderLoopIntervalInSecond, "")) {
        BS_LOG(ERROR, "Admin init leader checker failed, zkRoot[%s]", zkRoot.c_str());
        BEEPER_REPORT(ADMIN_STATUS_COLLECTOR_NAME, "admin worker start fail, init leader checker failed!");
        return false;
    }
    if (!waitToBeLeader()) {
        BS_LOG(ERROR, "failed to be leader!");
        BEEPER_REPORT(ADMIN_STATUS_COLLECTOR_NAME, "admin worker start fail, fail to be leader!");
        return false;
    }

    if (!startMonitor()) {
        BS_LOG(WARN, "start monitor failed");
        BEEPER_REPORT(ADMIN_STATUS_COLLECTOR_NAME, "admin worker start fail, start monitor failed!");
        return false;
    }

    if (!initLocalLuaPath(zkRoot)) {
        BS_LOG(ERROR, "init local lua path failed");
        BEEPER_REPORT(ADMIN_STATUS_COLLECTOR_NAME, "admin worker start fail, init local lua path failed!");
        return false;
    }

    if (!setLeaderInfo()) {
        BEEPER_REPORT(ADMIN_STATUS_COLLECTOR_NAME, "admin worker start fail, set leader info failed!");
        BS_LOG(ERROR, "set leader info failed");
        return false;
    }

    if (!_keeper->start(zkRoot, adminServiceName, getZkWrapper(), createSimpleMasterScheduler(), amonitorPort,
                        prohibitTime, prohibitNum, _monitor)) {
        BS_LOG(ERROR, "start ServiceKeeper failed!");
        BEEPER_REPORT(ADMIN_STATUS_COLLECTOR_NAME, "admin worker start fail, start ServiceKeeper failed!");
        return false;
    }

    _keeper->setHttpAddress(getHTTPAddress());
    string hippoRoot;
    getOptionParser().getOptionValue("hippo_root", hippoRoot);
    _keeper->setHippoZkRoot(hippoRoot);

    lc->done();
    _isStarted = true;
    BS_LOG(INFO, "admin worker started");
    BEEPER_REPORT(ADMIN_STATUS_COLLECTOR_NAME, "admin worker started");
    return true;
}

bool AdminWorker::setLeaderInfo()
{
    worker_framework::LeaderInfo info;
    string ip = getIp();
    uint32_t port = getHTTPPort();
    info.setHttpAddress(ip, port);
    info.set("port", getPort());
    info.set("address", getAddress());

    string infoStr = info.toString();
    worker_framework::LeaderElector* elector = getLeaderElector();
    bool ret = elector->writeLeaderInfo(getLeaderElectorPath(), infoStr) &&
               elector->writeLeaderInfo(getLeaderInfoPath(), infoStr);
    if (ret) {
        AdminInfo* adminInfo = AdminInfo::GetInstance();
        adminInfo->setAdminAddress(ip);
        adminInfo->setAdminHttpPort(port);
    }
    return ret;
}

void AdminWorker::doStop()
{
    BEEPER_REPORT(ADMIN_STATUS_COLLECTOR_NAME, "admin worker stopping");
    stopRPCServer();
    DELETE_AND_SET_NULL(_keeper);
    shutdownLeaderElector();
    BEEPER_CLOSE();
}

void AdminWorker::startBuild(::google::protobuf::RpcController* controller, const proto::StartBuildRequest* request,
                             proto::StartBuildResponse* response, ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "start build request[%s]", request->ShortDebugString().c_str());
    _keeper->startBuild(request, response);
}

void AdminWorker::startTask(::google::protobuf::RpcController* controller, const proto::StartTaskRequest* request,
                            proto::InformResponse* response, ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "start task request[%s]", request->ShortDebugString().c_str());
    _keeper->startTask(request, response);
}

void AdminWorker::stopTask(::google::protobuf::RpcController* controller, const proto::StopTaskRequest* request,
                           proto::InformResponse* response, ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "stop task request[%s]", request->ShortDebugString().c_str());
    _keeper->stopTask(request, response);
}

void AdminWorker::stopBuild(::google::protobuf::RpcController* controller, const proto::StopBuildRequest* request,
                            proto::InformResponse* response, ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "stop build request[%s]", request->ShortDebugString().c_str());
    _keeper->stopBuild(request, response);
}

void AdminWorker::updateConfig(::google::protobuf::RpcController* controller, const proto::UpdateConfigRequest* request,
                               proto::InformResponse* response, ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "update config request[%s]", request->ShortDebugString().c_str());
    _keeper->updateConfig(request, response);
}

void AdminWorker::stepBuild(::google::protobuf::RpcController* controller, const proto::StepBuildRequest* request,
                            proto::InformResponse* response, ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_INTERVAL_LOG(30, INFO, "step build request[%s]", request->ShortDebugString().c_str());
    _keeper->stepBuild(request, response);
}

void AdminWorker::suspendBuild(::google::protobuf::RpcController* controller, const proto::SuspendBuildRequest* request,
                               proto::InformResponse* response, ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "suspend build request[%s]", request->ShortDebugString().c_str());
    _keeper->suspendBuild(request, response);
}

void AdminWorker::resumeBuild(::google::protobuf::RpcController* controller, const proto::ResumeBuildRequest* request,
                              proto::InformResponse* response, ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "resume build request[%s]", request->ShortDebugString().c_str());
    _keeper->resumeBuild(request, response);
}

void AdminWorker::rollBack(::google::protobuf::RpcController* controller, const proto::RollBackRequest* request,
                           proto::InformResponse* response, ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "rollBack request[%s]", request->ShortDebugString().c_str());
    _keeper->rollBack(request, response);
}

void AdminWorker::rollBackCheckpoint(::google::protobuf::RpcController* controller,
                                     const proto::RollBackCheckpointRequest* request, proto::InformResponse* response,
                                     ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "rollback checkpoint request[%s]", request->ShortDebugString().c_str());
    _keeper->rollBackCheckpoint(request, response);
}

void AdminWorker::createSnapshot(::google::protobuf::RpcController* controller,
                                 const proto::CreateSnapshotRequest* request, proto::InformResponse* response,
                                 ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "createSnapshot request[%s]", request->ShortDebugString().c_str());
    _keeper->createSnapshot(request, response);
}

void AdminWorker::removeSnapshot(::google::protobuf::RpcController* controller,
                                 const proto::RemoveSnapshotRequest* request, proto::InformResponse* response,
                                 ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "removeSnapshot request[%s]", request->ShortDebugString().c_str());
    _keeper->removeSnapshot(request, response);
}

void AdminWorker::createSavepoint(::google::protobuf::RpcController* controller,
                                  const proto::CreateSavepointRequest* request, proto::InformResponse* response,
                                  ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "createSavepoint request[%s]", request->ShortDebugString().c_str());
    _keeper->createSavepoint(request, response);
}

void AdminWorker::removeSavepoint(::google::protobuf::RpcController* controller,
                                  const proto::RemoveSavepointRequest* request, proto::InformResponse* response,
                                  ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "removeSavepoint request[%s]", request->ShortDebugString().c_str());
    _keeper->removeSavepoint(request, response);
}

void AdminWorker::getCheckpoint(::google::protobuf::RpcController* controller,
                                const proto::GetCheckpointRequest* request, proto::InformResponse* response,
                                ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "getCheckpoint request[%s]", request->ShortDebugString().c_str());
    _keeper->getCheckpoint(request, response);
}

void AdminWorker::listCheckpoint(::google::protobuf::RpcController* controller,
                                 const proto::ListCheckpointRequest* request, proto::InformResponse* response,
                                 ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "listCheckpoint request[%s]", request->ShortDebugString().c_str());
    _keeper->listCheckpoint(request, response);
}

void AdminWorker::commitVersion(::google::protobuf::RpcController* controller,
                                const proto::CommitVersionRequest* request, proto::InformResponse* response,
                                ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "commitVersion request[%s]", request->ShortDebugString().c_str());
    _keeper->commitVersion(request, response);
}

void AdminWorker::getCommittedVersions(::google::protobuf::RpcController* controller,
                                       const proto::GetCommittedVersionsRequest* request,
                                       proto::InformResponse* response, ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "getCommittedVersions request[%s]", request->ShortDebugString().c_str());
    _keeper->getCommittedVersions(request, response);
}

void AdminWorker::createVersion(::google::protobuf::RpcController* controller,
                                const proto::CreateVersionRequest* request, proto::InformResponse* response,
                                ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "create version request[%s]", request->ShortDebugString().c_str());
    _keeper->createVersion(request, response);
}

void AdminWorker::switchBuild(::google::protobuf::RpcController* controller, const proto::SwitchBuildRequest* request,
                              proto::InformResponse* response, ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "switch build request[%s]", request->ShortDebugString().c_str());
    _keeper->switchBuild(request, response);
}

void AdminWorker::callGraph(::google::protobuf::RpcController* controller, const proto::CallGraphRequest* request,
                            proto::InformResponse* response, ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "call graph request[%s]", request->ShortDebugString().c_str());
    _keeper->callGraph(request, response);
}

void AdminWorker::printGraph(::google::protobuf::RpcController* controller, const proto::PrintGraphRequest* request,
                             proto::InformResponse* response, ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "print graph request[%s]", request->ShortDebugString().c_str());
    _keeper->printGraph(request, response);
}

void AdminWorker::updateGsTemplate(::google::protobuf::RpcController* controller,
                                   const proto::UpdateGsTemplateRequest* request, proto::InformResponse* response,
                                   ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "update gs template request[%s]", request->ShortDebugString().c_str());
    _keeper->updateGsTemplate(request, response);
}

void AdminWorker::updateGlobalAgentConfig(::google::protobuf::RpcController* controller,
                                          const proto::UpdateGlobalAgentConfigRequest* request,
                                          proto::InformResponse* response, ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "update global agent config request[%s]", request->ShortDebugString().c_str());
    _keeper->updateGlobalAgentConfig(request, response);
}

void AdminWorker::getTaskInfo(::google::protobuf::RpcController* controller, const proto::TaskInfoRequest* request,
                              proto::TaskInfoResponse* response, ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    _keeper->fillTaskInfo(request, response);
}

void AdminWorker::getWorkerRoleInfo(::google::protobuf::RpcController* controller,
                                    const proto::WorkerRoleInfoRequest* request,
                                    proto::WorkerRoleInfoResponse* response, ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    _keeper->fillWorkerRoleInfo(request, response);
}

void AdminWorker::getWorkerNodeStatus(::google::protobuf::RpcController* controller,
                                      const proto::WorkerNodeStatusRequest* request,
                                      proto::WorkerNodeStatusResponse* response, ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    _keeper->fillWorkerNodeStatus(request, response);
}

void AdminWorker::getServiceInfo(::google::protobuf::RpcController* controller,
                                 const proto::ServiceInfoRequest* request, proto::ServiceInfoResponse* response,
                                 ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    response->set_adminaddress(getAddress());
    _keeper->fillServiceInfo(request, response);
}

void AdminWorker::setLoggerLevel(::google::protobuf::RpcController* controller,
                                 const proto::SetLoggerLevelRequest* request, proto::InformResponse* response,
                                 ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    alog::Logger* logger = alog::Logger::getLogger(request->logger().c_str());
    if (!logger) {
        response->add_errormessage("logger is null");
        BS_LOG(ERROR, "logger [%s] is null", request->logger().c_str());
    }
    logger->setLevel(request->level());
}

void AdminWorker::cleanVersions(::google::protobuf::RpcController* controller,
                                const proto::CleanVersionsRequest* request, proto::InformResponse* response,
                                ::google::protobuf::Closure* done)
{
    RPC_GUARD();

    BS_LOG(INFO, "clean versions request[%s]", request->ShortDebugString().c_str());
    _keeper->cleanVersions(request, response);
}

void AdminWorker::startGeneralTask(::google::protobuf::RpcController* controller,
                                   const proto::StartGeneralTaskRequest* request, proto::InformResponse* response,
                                   ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO,
           "start general task request, buildId[%s], taskId[%ld], partitionIndexRoot[%s], "
           "taskEpochId[%s], sourceVersionId[%ld], branchId [%ld]",
           request->buildid().ShortDebugString().c_str(), request->taskid(), request->partitionindexroot().c_str(),
           request->taskepochid().c_str(), request->sourceversionid(), request->branchid());
    _keeper->startGeneralTask(request, response);
}

void AdminWorker::migrateTargetRoles(::google::protobuf::RpcController* controller,
                                     const proto::MigrateRoleRequest* request, proto::InformResponse* response,
                                     ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "migrate target roles request[%s]", request->ShortDebugString().c_str());
    _keeper->migrateTargetRoles(request, response);
}

void AdminWorker::getGeneralInfo(::google::protobuf::RpcController* controller, const proto::InformRequest* request,
                                 proto::InformResponse* response, ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "get general info request[%s]", request->ShortDebugString().c_str());
    _keeper->getGeneralInfo(request, response);
}

void AdminWorker::getBulkloadInfo(::google::protobuf::RpcController* controller,
                                  const proto::BulkloadInfoRequest* request, proto::InformResponse* response,
                                  ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "get bulkload info request[%s]", request->ShortDebugString().c_str());
    _keeper->getBulkloadInfo(request, response);
}

void AdminWorker::bulkload(::google::protobuf::RpcController* controller, const proto::BulkloadRequest* request,
                           proto::InformResponse* response, ::google::protobuf::Closure* done)
{
    RPC_GUARD();
    BS_LOG(INFO, "bulkload request[%s]", request->ShortDebugString().c_str());
    _keeper->bulkload(request, response);
}

void AdminWorker::doIdle()
{
    if (!isLeader()) {
        const string& address = getAddress();
        string msg = string("AdminWorker[") + address + "] is no longer leader, exit now";
        cerr << msg << endl;

        BEEPER_REPORT(ADMIN_STATUS_COLLECTOR_NAME, msg);
        BEEPER_CLOSE();
        _exit(-1);
    }
}

void AdminWorker::noLongerLeader()
{
    const string& address = getAddress();
    string msg = string("AdminWorker[") + address + "] is no longer leader, exit now";
    cerr << msg << endl;

    BEEPER_REPORT(ADMIN_STATUS_COLLECTOR_NAME, msg);
    BEEPER_CLOSE();
    _exit(-1);
}

SimpleMasterScheduler* AdminWorker::createSimpleMasterScheduler() const
{
    bool localMode = false;
    getOptionParser().getOptionValue("local_mode", localMode);
    if (localMode) {
        BS_LOG(INFO, "bs admin start with local mode");
    }

    string zkRoot;
    getOptionParser().getOptionValue("zookeeper_root", zkRoot);

    string appName;
    string hippoRoot;
    getOptionParser().getOptionValue("app_name", appName);
    getOptionParser().getOptionValue("hippo_root", hippoRoot);

    std::unique_ptr<SimpleMasterScheduler> scheduler;
    if (localMode) {
        scheduler = make_unique<SimpleMasterSchedulerLocal>(getCwdPath(), getLogConfFile());
    } else {
        scheduler = make_unique<SimpleMasterScheduler>();
    }
    if (autil::EnvUtil::getEnv("BS_SUPPORT_AGENT_NODES", true)) {
        BS_LOG(INFO, "bs admin start with agent_nodes enabled");
        std::shared_ptr<SimpleMasterScheduler> innerScheduler(scheduler.release());
        scheduler = make_unique<AgentSimpleMasterScheduler>(zkRoot, getZkWrapper(), innerScheduler, _monitor);
    }
    if (!scheduler->init(hippoRoot, getLeaderElector(), appName)) {
        BS_LOG(ERROR, "Init hippo master scheduler failed. hippoRoot[%s], appId[%s]", hippoRoot.c_str(),
               appName.c_str());
        return NULL;
    }
    return scheduler.release();
}

bool AdminWorker::initLocalLuaPath(const string& zkRoot) { return LocalLuaScriptReader::initLocalPath(zkRoot); }

void AdminWorker::initBeeper()
{
    string beeperConfigPath;
    getOptionParser().getOptionValue("beeper_config_path", beeperConfigPath);
    if (beeperConfigPath.empty()) {
        char buffer[1024];
        int ret = readlink("/proc/self/exe", buffer, sizeof(buffer) - 1);
        // .../usr/local/bin
        string binaryRoot = fslib::util::FileUtil::getParentDir(string(buffer, ret));
        // .../usr/local/etc/bs/bs_admin_beeper.json
        beeperConfigPath = fslib::util::FileUtil::joinFilePath(fslib::util::FileUtil::getParentDir(binaryRoot),
                                                               "etc/bs/bs_admin_beeper.json");
        BS_LOG(INFO, "not set beeper_config_path, use local path beeper config [%s]", beeperConfigPath.c_str());
    } else {
        BS_LOG(INFO, "use beeper_config_path parameter [%s]", beeperConfigPath.c_str());
    }

    string content;
    if (!fslib::util::FileUtil::readFile(beeperConfigPath, content)) {
        BS_LOG(WARN, "init beeper error, read beeper config file [%s] fail!", beeperConfigPath.c_str());
        return;
    }

    string adminServiceName;
    getOptionParser().getOptionValue("app_name", adminServiceName);

    beeper::EventTags globalTags;
    globalTags.AddTag("appName", adminServiceName);
    BEEPER_INIT_FROM_CONF_STRING(content, globalTags);

    DECLARE_BEEPER_COLLECTOR(ADMIN_STATUS_COLLECTOR_NAME);
    DECLARE_BEEPER_COLLECTOR(GENERATION_STATUS_COLLECTOR_NAME);
    DECLARE_BEEPER_COLLECTOR(GENERATION_ERROR_COLLECTOR_NAME);
    DECLARE_BEEPER_COLLECTOR(GENERATION_CMD_COLLECTOR_NAME);
    DECLARE_BEEPER_COLLECTOR(BATCHMODE_INFO_COLLECTOR_NAME);

    DECLARE_BEEPER_COLLECTOR(FSLIB_REMOVE_COLLECTOR_NAME);
    DECLARE_BEEPER_COLLECTOR(FSLIB_ERROR_CODE_NOT_SUPPORT);
    DECLARE_BEEPER_COLLECTOR(FSLIB_LONG_INTERVAL_COLLECTOR_NAME);
    // TODO: add more collector as needed
}

}} // namespace build_service::admin
