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
#include "build_service_tasks/syncIndex/SyncIndexTask.h"

#include <algorithm>
#include <functional>
#include <google/protobuf/stubs/port.h>
#include <iosfwd>
#include <unistd.h>

#include "alog/Logger.h"
#include "autil/LoopThread.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/config/BuildRuleConfig.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/RangeUtil.h"
#include "build_service_tasks/channel/MadroxChannel.h"
#include "build_service_tasks/channel/Master.pb.h"

using namespace std;
using namespace google::protobuf;
using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::util;
using namespace autil::legacy;
using namespace autil;

namespace build_service { namespace task_base {

BS_LOG_SETUP(task_base, SyncIndexTask);

const string SyncIndexTask::TASK_NAME = BS_TASK_SYNC_INDEX;

SyncIndexTask::SyncIndexTask() : _hasFatalError(false), _requestPosted(false) {}

bool SyncIndexTask::init(TaskInitParam& initParam)
{
    _taskInitParam = initParam;
    auto configReader = _taskInitParam.resourceReader;
    if (!configReader) {
        BS_LOG(ERROR, "failed to get config reader");
        return false;
    }
    if (initParam.clusterName.empty()) {
        string errorMsg = "partition id has no cluster name";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return false;
    }
    BuildRuleConfig buildRuleConfig;
    if (!_taskInitParam.resourceReader->getClusterConfigWithJsonPath(
            _taskInitParam.clusterName, "cluster_config.builder_rule_config", buildRuleConfig)) {
        string errorMsg = "parse cluster_config.builder_rule_config for [" + _taskInitParam.clusterName + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _partitionCount = buildRuleConfig.partitionCount;

    config::BuildServiceConfig buildServiceConfig;
    if (!configReader->getConfigWithJsonPath("build_app.json", "", buildServiceConfig)) {
        string errorMsg = "failed to get build_app.json in config[" + configReader->getConfigPath() + "]";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    _sourceIndexRoot = buildServiceConfig.getIndexRoot();
    _sourceIndexRoot = util::IndexPathConstructor::getGenerationIndexPath(_sourceIndexRoot, _taskInitParam.clusterName,
                                                                          _taskInitParam.buildId.generationId);
    _updateThread =
        LoopThread::createLoopThread(bind(&SyncIndexTask::updateLoop, this), 15 * 1000 * 1000, "updateThread"); // 15s
    if (!_updateThread) {
        BS_LOG(ERROR, "create update thread failed");
        return false;
    }
    return true;
}

bool SyncIndexTask::isDone(const config::TaskTarget& target)
{
    vector<versionid_t> versions;
    ScopedLock lk(_reachTargetMtx);
    return target == _reachedTarget && checkTarget(_reachedTarget.getTargetDescription(), versions);
}
indexlib::util::CounterMapPtr SyncIndexTask::getCounterMap() { return indexlib::util::CounterMapPtr(); }

string SyncIndexTask::getTaskStatus()
{
    ScopedLock lk(_reachTargetMtx);
    return ToJsonString(_reachedTarget.getTargetDescription());
}

void SyncIndexTask::updateLoop()
{
    usleep(0);
    ScopedLock lk(_mtx);
    vector<versionid_t> targetVersions;
    if (_hasFatalError) {
        BS_LOG(ERROR, "has fatal error, no need to update");
        return;
    }
    if (_reachedTarget == _currentTarget) {
        BS_LOG(INFO, "already reach target, no need to update");
        return;
    }
    if (!checkTarget(_currentTarget.getTargetDescription(), targetVersions)) {
        BS_LOG(INFO, "current target invalid, no need to update");
        return;
    }
    if (!_requestPosted) {
        if (!cloneIndexRpcBlock(targetVersions)) {
            string errorMsg = "clone target versions " + ToJsonString(targetVersions) + " failed";
            REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
            _hasFatalError = true;
            return;
        } else {
            _requestPosted = true;
        }
    }

    for (size_t i = 0; i < GS_RETRY_LIMIT; ++i) {
        auto ret = isAllSynced(targetVersions);
        if (ret == SyncStatus::SS_DONE) {
            ScopedLock lk(_reachTargetMtx);
            _reachedTarget = _currentTarget;
            return;
        }
        if (ret == SyncStatus::SS_FAILED || ret == SyncStatus::SS_RETRY) {
            ScopedLock lk(_reachTargetMtx);
            _requestPosted = false;
            _reachedTarget.updateTargetDescription("need_update_target", KV_TRUE);
        }
        sleep(RPC_RETRY_INTERVAL_S);
    }
}

bool SyncIndexTask::handleTarget(const config::TaskTarget& target)
{
    const KeyValueMap& targetDesc = target.getTargetDescription();
    vector<versionid_t> targetVersions;
    if (!checkTarget(targetDesc, targetVersions)) {
        string errorMsg = "task params is invalid";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return false;
    }
    ScopedLock lk(_mtx);
    _reachedTarget.removeTargetDescription("need_update_target");
    if (target == _reachedTarget) {
        BS_LOG(INFO, "already reach target");
        return true;
    }
    string madroxRoot = getValueFromKeyValueMap(target.getTargetDescription(), MADROX_ADMIN_ADDRESS);
    if (madroxRoot != _madroxRoot) {
        config::TaskTarget emptyTarget;
        _reachedTarget = emptyTarget;
        _madroxRoot = madroxRoot;
        _madroxChannel = make_shared<MadroxChannel>(_madroxRoot);
        if (!_madroxChannel->init()) {
            return false;
        }
    }
    string indexRoot = getValueFromKeyValueMap(target.getTargetDescription(), BS_SYNC_INDEX_ROOT);
    indexRoot = util::IndexPathConstructor::getGenerationIndexPath(indexRoot, _taskInitParam.clusterName,
                                                                   _taskInitParam.buildId.generationId);
    if (indexRoot != _remoteIndexRoot) {
        config::TaskTarget emptyTarget;
        _reachedTarget = emptyTarget;
        _remoteIndexRoot = indexRoot;
    }
    _currentTarget = target;
    _requestPosted = false;
    return true;
}

SyncIndexTask::SyncStatus SyncIndexTask::isAllSynced(const vector<versionid_t>& targetVersions)
{
    if (_remoteIndexRoot == _sourceIndexRoot) {
        BS_LOG(INFO, "source index root [%s] same as remote index root[%s], all index synced", _sourceIndexRoot.c_str(),
               _remoteIndexRoot.c_str());
        return SyncStatus::SS_DONE;
    }
    ::madrox::proto::GetStatusRequest request;
    ::madrox::proto::GetStatusResponse response;
    auto deployMetas = getDeployMetas(targetVersions);
    *request.add_destroot() = _remoteIndexRoot;
    for (const auto& meta : deployMetas) {
        *request.add_deploymeta() = meta;
    }
    BS_LOG(INFO, "begin check status [%s]", request.ShortDebugString().c_str());
    if (!_madroxChannel->GetStatus(request, response)) {
        BS_LOG(WARN, "get status failed");
        return SyncStatus::SS_RETRY;
    }
    const auto& err = response.errorinfo();
    if (err.errorcode() != 0) {
        BS_LOG(ERROR, "update request failed, madrox path:[%s], error:[%d:%s]", _madroxRoot.c_str(), err.errorcode(),
               err.errormsg().c_str());
        return SyncStatus::SS_FAILED;
    }
    if (response.dest().size() != 1) {
        BS_LOG(ERROR, "response dest size is [%u] != 1", response.dest().size());
        return SyncStatus::SS_FAILED;
    }
    const auto& deployDetail = response.dest().Get(0).deploymetadetail();
    if (deployDetail.size() != deployMetas.size()) {
        BS_LOG(ERROR, "response size not match response [%lu] vs request [%lu]", (size_t)deployDetail.size(),
               (size_t)deployMetas.size());
        return SyncStatus::SS_FAILED;
    }
    size_t doneCount = 0;
    for (size_t i = 0; i < deployDetail.size(); ++i) {
        auto detail = deployDetail.Get(i);
        auto status = detail.status();
        if (status == madrox::proto::SyncStatus::FAILED) {
            BS_LOG(ERROR, "some item failed [%s]", detail.ShortDebugString().c_str());
            return SyncStatus::SS_FAILED;
        }
        if (status == madrox::proto::SyncStatus::NEED_RETRY || status == madrox::proto::SyncStatus::CANCELED ||
            status == madrox::proto::SyncStatus::LOCKED) {
            BS_LOG(ERROR, "some item failed [%s]", detail.ShortDebugString().c_str());
            return SyncStatus::SS_RETRY;
        }
        if (status == madrox::proto::SyncStatus::DONE) {
            doneCount++;
        }
    }
    BS_LOG(INFO, "target meta count [%u], done count [%lu]", deployDetail.size(), doneCount);
    if (doneCount == deployMetas.size()) {
        return SyncStatus::SS_DONE;
    } else {
        return SyncStatus::SS_WAIT;
    }
}
vector<string> SyncIndexTask::getDeployMetas(const vector<versionid_t>& targetVersions)
{
    std::vector<Range> partRanges = RangeUtil::splitRange(RANGE_MIN, RANGE_MAX, _partitionCount);

    std::vector<std::string> deployMetas;
    for (const auto& targetVersion : targetVersions) {
        for (const auto& partRange : partRanges) {
            std::string partMeta = "partition_" + std::to_string(partRange.from()) + "_" +
                                   std::to_string(partRange.to()) + "/deploy_meta." + std::to_string(targetVersion);
            deployMetas.emplace_back(partMeta);
        }
    }
    return deployMetas;
}

bool SyncIndexTask::cloneIndexRpcBlock(const vector<versionid_t>& targetVersions)
{
    if (_remoteIndexRoot == _sourceIndexRoot) {
        BS_LOG(INFO, "source index root [%s] same as remote index root[%s], no need to sync", _sourceIndexRoot.c_str(),
               _remoteIndexRoot.c_str());
        return true;
    }
    ::madrox::proto::UpdateRequest request;
    ::madrox::proto::UpdateResponse response;
    auto deployMetas = getDeployMetas(targetVersions);
    *request.add_destroot() = _remoteIndexRoot;
    request.set_srcroot(_sourceIndexRoot);
    request.set_removeouter(true);
    for (const auto& meta : deployMetas) {
        *request.add_deploymeta() = meta;
    }
    if (!_madroxChannel->UpdateRequest(request, response)) {
        return false;
    }
    const auto& err = response.errorinfo();
    if (err.errorcode() != 0) {
        BS_LOG(ERROR, "update request failed, madrox path:[%s], error:[%d:%s]", _madroxRoot.c_str(), err.errorcode(),
               err.errormsg().c_str());
        return false;
    }
    BS_LOG(INFO, "update request success, request[%s], clusterName[%s], address = [%s]",
           request.ShortDebugString().c_str(), _taskInitParam.clusterName.c_str(),
           _madroxChannel->getHostAddress().c_str());
    return true;
}

bool SyncIndexTask::checkTarget(const KeyValueMap& kvMap, vector<versionid_t>& targetVersions) const
{
    if (kvMap.size() == 0) {
        return false;
    }
    string madroxAddr = getValueFromKeyValueMap(kvMap, MADROX_ADMIN_ADDRESS);
    if (madroxAddr.empty()) {
        BS_LOG(ERROR, "madrox addr is empty, desc [%s]", ToJsonString(kvMap).c_str());
        return false;
    }

    string remoteIndexRoot = getValueFromKeyValueMap(kvMap, BS_SYNC_INDEX_ROOT);
    if (remoteIndexRoot.empty()) {
        BS_LOG(ERROR, "sync index root  is empty, desc [%s]", ToJsonString(kvMap).c_str());
        return false;
    }

    string versionStr = getValueFromKeyValueMap(kvMap, "versions");
    try {
        if (!versionStr.empty()) {
            FromJsonString(targetVersions, versionStr);
        } else {
            BS_LOG(ERROR, "version str is empty");
            return false;
        }
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "INVALID json str[%s]", versionStr.c_str());
        return false;
    }
    sort(targetVersions.begin(), targetVersions.end());
    if (targetVersions.empty()) {
        BS_LOG(ERROR, "target version is empty");
        return false;
    }
    if (targetVersions[0] < 0) {
        BS_LOG(ERROR, "invalid version [%s]", ToJsonString(targetVersions).c_str());
        return false;
    }

    return true;
}
}} // namespace build_service::task_base
