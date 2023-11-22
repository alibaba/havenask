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
#include "build_service_tasks/cloneIndex/CloneIndexTask.h"

#include <google/protobuf/stubs/port.h>
#include <google/protobuf/stubs/status.h>
#include <iosfwd>
#include <stdint.h>
#include <unistd.h>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "build_service/config/BuildRuleConfig.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/util/RangeUtil.h"
#include "build_service_tasks/channel/Master.pb.h"
#include "fslib/util/FileUtil.h"
#include "google/protobuf/util/json_util.h"

using namespace std;
using namespace google::protobuf;
using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::util;
using namespace autil::legacy;
using namespace autil;

namespace build_service { namespace task_base {

// using BSMadrox = ::madrox::proto;
BS_LOG_SETUP(task_base, CloneIndexTask);

const string CloneIndexTask::TASK_NAME = BS_TASK_CLONE_INDEX;
const string CloneIndexTask::CLONE_INDEX_DESCRIPTION_FILE = "__clone_index_description__";

CloneIndexTask::CloneIndexTask() : _isFinished(false), _targetInfoInited(false) {}

bool CloneIndexTask::init(TaskInitParam& initParam)
{
    _taskInitParam = initParam;
    if (initParam.clusterName.empty()) {
        string errorMsg = "partition id has no cluster name";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return false;
    }
    return true;
}

bool CloneIndexTask::isDone(const config::TaskTarget& target) { return _isFinished; }

indexlib::util::CounterMapPtr CloneIndexTask::getCounterMap() { return indexlib::util::CounterMapPtr(); }

string CloneIndexTask::getTaskStatus() { return ToJsonString(_kvMap); }

bool CloneIndexTask::writeCheckpointFiles(const ClusterMap& targetClusterMap)
{
    for (const auto& kv : targetClusterMap) {
        bool exist = false;
        if (!fslib::util::FileUtil::isExist(kv.second.targetIndexPath, exist)) {
            BS_LOG(ERROR, "IO error: failed to determine existence of path[%s]", kv.second.targetIndexPath.c_str());
            return false;
        }
        if (!exist) {
            if (!fslib::util::FileUtil::mkDir(kv.second.targetIndexPath, true)) {
                BS_LOG(ERROR, "IO error: failed to mkDir of path[%s]", kv.second.targetIndexPath.c_str());
                return false;
            }
        }
        auto checkpointFilePath =
            fslib::util::FileUtil::joinFilePath(kv.second.targetIndexPath, CLONE_INDEX_DESCRIPTION_FILE);
        if (!fslib::util::FileUtil::removeIfExist(checkpointFilePath)) {
            BS_LOG(ERROR, "IO error: failed to remove of path[%s]", checkpointFilePath.c_str());
            return false;
        }
        auto clusterDescInJson = ToJsonString(kv.second);
        if (!fslib::util::FileUtil::writeFile(checkpointFilePath, clusterDescInJson)) {
            BS_LOG(ERROR, "IO error: failed to write file of path[%s], content[%s]", checkpointFilePath.c_str(),
                   clusterDescInJson.c_str());
            return false;
        }
        BS_LOG(INFO, "write clusterDescription[%s] to checkpointPath[%s] done", clusterDescInJson.c_str(),
               checkpointFilePath.c_str());
    }
    return true;
}

bool CloneIndexTask::tryRecoverFromCheckpointFiles(const vector<pair<string, versionid_t>>& clusterInfos,
                                                   ClusterMap& targetClusterMap, bool& recovered)
{
    for (const auto& clusterInfo : clusterInfos) {
        auto indexPath = getGenerationPath(_indexRoot, clusterInfo.first, _buildId.generationid());
        bool exist = false;
        if (!fslib::util::FileUtil::isExist(indexPath, exist)) {
            BS_LOG(ERROR, "IO error: failed to determin existence of path[%s]", indexPath.c_str());
            return false;
        }
        if (!exist) {
            BS_LOG(INFO, "recover failed: index path[%s] not existed", indexPath.c_str());
            recovered = false;
            return true;
        }
        auto checkpointFilePath = fslib::util::FileUtil::joinFilePath(indexPath, CLONE_INDEX_DESCRIPTION_FILE);
        bool ckpExist = false;
        if (!fslib::util::FileUtil::isExist(checkpointFilePath, ckpExist)) {
            BS_LOG(ERROR, "IO error: failed to determin existence of path[%s]", checkpointFilePath.c_str());
            return false;
        }
        if (!ckpExist) {
            BS_LOG(INFO, "recover failed: checkpoint path[%s] not existed", checkpointFilePath.c_str());
            recovered = false;
            return true;
        }
        string clusterDescInJson;
        if (!fslib::util::FileUtil::readFile(checkpointFilePath, clusterDescInJson)) {
            BS_LOG(INFO, "IO error: read from checkpoint path[%s] failed", checkpointFilePath.c_str());
            return false;
        }
        ClusterDescription clusterDesc;
        try {
            autil::legacy::FromJsonString(clusterDesc, clusterDescInJson);
        } catch (autil::legacy::ExceptionBase& e) {
            BS_LOG(ERROR, "recover failed: FromJsonString [%s] catch exception: [%s]", clusterDescInJson.c_str(),
                   e.ToString().c_str());
            recovered = false;
            return true;
        }
        targetClusterMap.insert(make_pair(clusterInfo.first, clusterDesc));
        BS_LOG(INFO, "recover cluster [%s] done", clusterInfo.first.c_str());
    }
    recovered = true;
    return true;
}

bool CloneIndexTask::initTargetInfo(const vector<pair<string, versionid_t>>& targetClusterInfos)
{
    if (_targetInfoInited) {
        return true;
    }

    if (targetClusterInfos.empty()) {
        BS_LOG(ERROR, "cannot init target info, no targetClusterInfos defined in target");
        return false;
    }
    ClusterMap targetMap;
    bool recovered = false;
    if (!tryRecoverFromCheckpointFiles(targetClusterInfos, targetMap, recovered)) {
        BS_LOG(ERROR, "try recover from checkpoint failed due to io exception");
        return false;
    }
    if (recovered) {
        BS_LOG(INFO, "success recovered targetClusterMap");
        _targetClusterMap = targetMap;
        _targetInfoInited = true;
        return true;
    }

    _targetClusterMap.clear();
    GenerationInfo generationInfo;
    if (!getServiceInfoRpcBlock(generationInfo)) {
        BS_LOG(ERROR, "issue rpc getServiceInfo to BsAdmin failed, path:[%s]", _srcIndexAdminPath.c_str());
        return false;
    }
    if (!checkAndGetClusterInfos(generationInfo)) {
        BS_LOG(ERROR, "invalid generationInfo, path:[%s] info[%s]", _srcIndexAdminPath.c_str(),
               generationInfo.ShortDebugString().c_str());
        return false;
    }
    auto resourceReader = _taskInitParam.resourceReader;
    bool hasSpecifiedVersion = false;
    for (const auto& clusterInfo : targetClusterInfos) {
        BuildRuleConfig buildRuleConfig;
        if (!resourceReader->getClusterConfigWithJsonPath(clusterInfo.first, "cluster_config.builder_rule_config",
                                                          buildRuleConfig)) {
            string errorMsg = "parse cluster_config.builder_rule_config for [" + clusterInfo.first + "] failed";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        ClusterDescription clusterDesc;
        clusterDesc.partitionCount = buildRuleConfig.partitionCount;
        clusterDesc.targetIndexPath = getGenerationPath(_indexRoot, clusterInfo.first, _buildId.generationid());
        uint32_t versionId = 0;
        int64_t locator = -1;
        if (clusterInfo.second != indexlib::INVALID_VERSIONID) {
            hasSpecifiedVersion = true;
            if (!getSpecifiedVersionLocator(generationInfo, clusterInfo.first, (uint32_t)clusterInfo.second, locator)) {
                BS_LOG(ERROR, "set versionId [%d] invalid for cluster:%s", clusterInfo.second,
                       clusterInfo.first.c_str());
                return false;
            }
            versionId = clusterInfo.second;
        } else if (!getLastVersionId(generationInfo, clusterInfo.first, versionId)) {
            BS_LOG(ERROR, "get last versionId failed for  cluster:%s", clusterInfo.first.c_str());
            return false;
        }
        clusterDesc.sourceVersionId = versionId;
        clusterDesc.sourceIndexPath =
            getGenerationPath(generationInfo.indexroot(), clusterInfo.first, _srcBuildId.generationid());
        if (locator != -1) {
            clusterDesc.indexLocator = locator;
        }
        _targetClusterMap.insert(make_pair(clusterInfo.first, clusterDesc));
    }
    if (!hasSpecifiedVersion) {
        for (int i = 0; i < generationInfo.indexinfos_size(); ++i) {
            const auto& indexInfo = generationInfo.indexinfos(i);
            const auto& clusterName = indexInfo.clustername();
            int64_t locator = 0;
            if (_controlConfig.isIncProcessorExist(clusterName)) {
                locator = indexInfo.processorcheckpoint();
            } else {
                locator = indexInfo.versiontimestamp();
            }
            // get min locator between partitions in one cluster
            auto it = _targetClusterMap.find(clusterName);
            if (it != _targetClusterMap.end() && it->second.sourceVersionId == indexInfo.indexversion()) {
                if (it->second.indexLocator == -1) {
                    it->second.indexLocator = locator;
                } else if (locator < (it->second.indexLocator)) {
                    it->second.indexLocator = locator;
                }
            }
        }
    }
    if (!writeCheckpointFiles(_targetClusterMap)) {
        BS_LOG(ERROR, "fail to serialize checkpoint files");
        _targetClusterMap.clear();
        return false;
    }
    _targetInfoInited = true;
    return true;
}

void CloneIndexTask::handleFatalError() { BS_LOG(WARN, "unexpected error happened. retry handleTarget later"); }

bool CloneIndexTask::handleTarget(const config::TaskTarget& target)
{
    if (_isFinished) {
        return true;
    }
    vector<pair<string, versionid_t>> targetClusterInfos;
    if (!getParamFromTaskTarget(target, targetClusterInfos)) {
        string errorMsg = "task params is invalid";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return false;
    }

    if (!_madroxChannel) {
        _madroxChannel = make_shared<MadroxChannel>(_madroxAdminPath);
        if (!_madroxChannel->init()) {
            return false;
        }
    }
    if (!_bsChannel) {
        _bsChannel = make_shared<BsAdminChannel>(_srcIndexAdminPath);
        if (!_bsChannel->init()) {
            return false;
        }
    }
    if (!initTargetInfo(targetClusterInfos)) {
        return false;
    }
    for (auto it = _targetClusterMap.begin(); it != _targetClusterMap.end(); ++it) {
        // 1. create snapshot
        // 2. copy index
        const auto& clusterName = it->first;
        versionid_t versionId = it->second.sourceVersionId;
        uint32_t partitionCount = it->second.partitionCount;
        if (!createSnapshot(clusterName, versionId)) {
            BS_LOG(ERROR, "create snapshot failed, cluster[%s], versionId[%u]", clusterName.c_str(), versionId);
            return false;
        }
        BS_LOG(INFO, "success createSnapshot for cluster[%s] versionId[%u]", clusterName.c_str(), versionId);

        if (!cloneIndexRpcBlock(clusterName)) {
            BS_LOG(ERROR, "madrox clone index failed, oriPath[%s] dstPath[%s] versionId[%u] partCnt[%u]",
                   it->second.sourceIndexPath.c_str(), it->second.targetIndexPath.c_str(), versionId, partitionCount);
            return false;
        }
        BS_LOG(INFO, "success clone index for cluster[%s] versionId[%u]", clusterName.c_str(), versionId);
    }

    uint32_t finishedTaskCount = 0;
    while (finishedTaskCount < _targetClusterMap.size()) {
        for (auto& kv : _targetClusterMap) {
            if (!kv.second.isDone) {
                if (getMadroxStatus(kv.first, kv.second.targetIndexPath) && removeSnapshots(kv.first)) {
                    kv.second.isDone = true;
                    finishedTaskCount++;
                    BS_LOG(INFO, "CloneIndexTask for Cluster[%s] is Done", kv.first.c_str());
                }
            }
        }
        sleep(RPC_RETRY_INTERVAL_S);
    }
    KeyValueMap cluster2Locator;
    for (const auto& kv : _targetClusterMap) {
        if (kv.second.indexLocator <= 0) {
            BS_LOG(ERROR, "no-valid indexLocator for cluster[%s]", kv.first.c_str());
            return false;
        }
        cluster2Locator[kv.first] = StringUtil::toString(kv.second.indexLocator);
    }
    auto locatorStr = ToJsonString(cluster2Locator);
    _kvMap[INDEX_CLONE_LOCATOR] = locatorStr;
    _isFinished = true;
    BS_LOG(INFO, "Done clone index task, cluster locator map[%s]", locatorStr.c_str());
    return true;
}

bool CloneIndexTask::getParamFromTaskTarget(const config::TaskTarget& target,
                                            vector<pair<string, versionid_t>>& targetClusterInfos)
{
    // get clone index param from targetDS
    if (!target.getTargetDescription(config::SOURCE_INDEX_ADMIN_ADDRESS, _srcIndexAdminPath)) {
        BS_LOG(ERROR, "missing source index admin addr");
        return false;
    }
    if (!target.getTargetDescription(config::SOURCE_INDEX_BUILD_ID, _srcIndexBuildId)) {
        BS_LOG(ERROR, "missing source index build id");
        return false;
    }
    if (!target.getTargetDescription(config::MADROX_ADMIN_ADDRESS, _madroxAdminPath)) {
        BS_LOG(ERROR, "missing madrox admin addr");
        return false;
    }
    std::string strClusterNames;
    if (!target.getTargetDescription("clusterNames", strClusterNames)) {
        BS_LOG(ERROR, "missing clusterNames");
        return false;
    }

    vector<string> clusters;
    if (!parseFromJsonString(strClusterNames, clusters)) {
        BS_LOG(ERROR, "invalid json for clusterNames field [%s]", strClusterNames.c_str());
        return false;
    }
    if (clusters.size() == 0) {
        BS_LOG(ERROR, "Empty clusterNames, no need issue cloneIndexTask at all");
        return false;
    }

    std::string versionIdStr;
    vector<versionid_t> versions;
    if (target.getTargetDescription("versionIds", versionIdStr)) {
        if (!parseFromJsonString(versionIdStr, versions)) {
            BS_LOG(ERROR, "parse version ids [%s] failed", versionIdStr.c_str());
            return false;
        }
    }

    if (versions.size() != 0 && versions.size() != clusters.size()) {
        BS_LOG(ERROR, "version size [%lu] not equal cluster size [%lu]", versions.size(), clusters.size());
        return false;
    }

    for (size_t i = 0; i < clusters.size(); i++) {
        versionid_t version = i >= versions.size() ? indexlib::INVALID_VERSIONID : versions[i];
        targetClusterInfos.push_back(make_pair(clusters[i], version));
    }

    BS_LOG(INFO, "success get bs_admin srcBuildId [%s] path[%s], madrox_admin path [%s]", _srcIndexBuildId.c_str(),
           _srcIndexAdminPath.c_str(), _madroxAdminPath.c_str());

    auto status = google::protobuf::util::JsonStringToMessage(_srcIndexBuildId, &_srcBuildId);
    if (!status.ok()) {
        BS_LOG(ERROR, "pb parse from string failed, buildId:[%s]", _srcIndexBuildId.c_str());
        return false;
    }

    _buildId.set_datatable(_taskInitParam.buildId.dataTable);
    _buildId.set_generationid(_taskInitParam.buildId.generationId);
    _buildId.set_appname(_taskInitParam.buildId.appName);

    BuildServiceConfig buildServiceConfig;
    auto resourceReader = _taskInitParam.resourceReader;
    if (!resourceReader->getDataTableConfigWithJsonPath(_buildId.datatable(), "control_config", _controlConfig)) {
        BS_LOG(ERROR, "get control_config.is_inc_processor_exist failed from dataTable[%s] failed",
               _buildId.datatable().c_str());
        return false;
    }
    if (!resourceReader->getConfigWithJsonPath("build_app.json", "", buildServiceConfig)) {
        BS_LOG(ERROR, "failed to get build_app.json in config[%s]", resourceReader->getConfigPath().c_str());
        return false;
    }
    _indexRoot = buildServiceConfig.getIndexRoot();
    if (_indexRoot.empty()) {
        BS_LOG(ERROR, "index root is empty.");
        return false;
    }
    return true;
}

bool CloneIndexTask::getMadroxStatus(const std::string& clusterName, const std::string& targetGenerationPath)
{
    ::madrox::proto::GetStatusRequest request;
    ::madrox::proto::GetStatusResponse response;

    request.add_destroot(targetGenerationPath);

    if (!_madroxChannel->GetStatus(request, response)) {
        return false;
    }

    const auto& err = response.errorinfo();
    if (err.errorcode() != 0) {
        BS_LOG(ERROR, "update request failed, madrox path:[%s], error:[%d:%s]", _madroxAdminPath.c_str(),
               err.errorcode(), err.errormsg().c_str());
        return false;
    }
    bool ret = false;
    const auto& status = response.status();
    if (response.dest_size() != 1) {
        BS_LOG(WARN, "unexpected dest size: [%u]", response.dest_size());
        return false;
    }

    auto dest = response.dest(0);
    BS_LOG(INFO, "cluster[%s] progress: finishedItemCount[%u], failedItemCount[%u], totalItemCount[%u]",
           clusterName.c_str(), dest.finisheditemcount(), dest.faileditemcount(), dest.totalitemcount());

    switch (status) {
    case ::madrox::proto::SyncStatus::RUNNING:
        ret = false;
        break;
    case ::madrox::proto::SyncStatus::CANCELED:
        BS_LOG(ERROR, "madrox task has been canceled, retry clone request");
        cloneIndexRpcBlock(clusterName);
        ret = false;
        break;
    case ::madrox::proto::SyncStatus::DONE:
        BS_LOG(INFO, "cluster[%s] clone done, initTimestamp[%ld], totalItemCount[%u]", clusterName.c_str(),
               dest.inittimestamp(), dest.totalitemcount());
        ret = true;
        break;
    default:
        ret = false;
        BS_LOG(ERROR, "unexpected madrox status, madrox path:[%s] status:[%d]", _madroxAdminPath.c_str(),
               static_cast<int>(status));
        cloneIndexRpcBlock(clusterName);
        break;
    }

    return ret;
}

bool CloneIndexTask::cloneIndexRpcBlock(const std::string& clusterName)
{
    ::madrox::proto::UpdateRequest request;
    ::madrox::proto::UpdateResponse response;
    auto it = _targetClusterMap.find(clusterName);
    if (it == _targetClusterMap.end()) {
        BS_LOG(ERROR, "target cluster[%s] not found", clusterName.c_str());
        return false;
    }

    if (it->second.isDone) {
        BS_LOG(WARN, "target cluster[%s] is Done. no need to copy", clusterName.c_str());
        return true;
    }

    std::vector<Range> partRanges = RangeUtil::splitRange(RANGE_MIN, RANGE_MAX, it->second.partitionCount);

    std::vector<std::string> deployMetas;
    for (const auto& partRange : partRanges) {
        std::string partMeta = "partition_" + std::to_string(partRange.from()) + "_" + std::to_string(partRange.to()) +
                               "/deploy_meta." + std::to_string(it->second.sourceVersionId);
        deployMetas.emplace_back(partMeta);
    }

    *request.add_destroot() = it->second.targetIndexPath;
    request.set_srcroot(it->second.sourceIndexPath);
    request.set_removeouter(false);
    for (const auto& meta : deployMetas) {
        *request.add_deploymeta() = meta;
    }
    if (!_madroxChannel->UpdateRequest(request, response)) {
        return false;
    }
    const auto& err = response.errorinfo();
    if (err.errorcode() != 0) {
        BS_LOG(ERROR, "update request failed, madrox path:[%s], error:[%d:%s]", _madroxAdminPath.c_str(),
               err.errorcode(), err.errormsg().c_str());
        return false;
    }
    BS_LOG(INFO, "update request success, request[%s], clusterName[%s], address = [%s]",
           request.ShortDebugString().c_str(), clusterName.c_str(), _madroxChannel->getHostAddress().c_str());
    return true;
}

bool CloneIndexTask::getServiceInfoRpcBlock(GenerationInfo& generationInfo)
{
    ServiceInfoRequest request;
    ServiceInfoResponse response;

    request.set_appname(_srcBuildId.appname());
    *request.mutable_buildid() = _srcBuildId;

    if (!_bsChannel->GetServiceInfo(request, response)) {
        return false;
    }
    if (!response.errormessage().empty()) {
        BS_LOG(ERROR, "getServiceInfo request failed, path:[%s], error:[%s]", _srcIndexAdminPath.c_str(),
               response.errormessage(0).c_str());
        return false;
    }

    if (response.generationinfos_size() != 1) {
        BS_LOG(ERROR, "generationInfos size[%d] is not 1", response.generationinfos_size());
        return false;
    }
    generationInfo = response.generationinfos(0);
    return true;
}

bool CloneIndexTask::createSnapshot(const std::string& clusterName, uint32_t versionId)
{
    CreateSnapshotRequest request;
    InformResponse response;

    request.set_clustername(clusterName);
    request.set_versionid(versionId);

    proto::BuildId buildId;
    auto status = google::protobuf::util::JsonStringToMessage(_srcIndexBuildId, &buildId);
    if (!status.ok()) {
        BS_LOG(ERROR, "pb parse from string failed, buildId:[%s]", _srcIndexBuildId.c_str());
        return false;
    }
    *request.mutable_buildid() = buildId;
    if (!_bsChannel->CreateSnapshot(request, response)) {
        return false;
    }

    if (!response.errormessage().empty()) {
        BS_LOG(ERROR, "createSnapshot request failed, path:[%s], error:[%s]", _srcIndexAdminPath.c_str(),
               response.errormessage(0).c_str());
        return false;
    }
    BS_LOG(INFO, "createSnapshot request[%s] done, server addr[%s]", request.ShortDebugString().c_str(),
           _bsChannel->getHostAddress().c_str());

    return true;
}

bool CloneIndexTask::removeSnapshots(const std::string& clusterName)
{
    RemoveSnapshotRequest request;
    InformResponse response;

    auto clusterIt = _targetClusterMap.find(clusterName);
    if (clusterIt == _targetClusterMap.end()) {
        BS_LOG(ERROR, "cluster[%s] not found in TargetClusterMap", clusterName.c_str());
        return false;
    }
    versionid_t snapshotVersion = clusterIt->second.sourceVersionId;
    request.set_clustername(clusterName);
    request.set_versionid(snapshotVersion);
    *request.mutable_buildid() = _srcBuildId;
    if (!_bsChannel->RemoveSnapshot(request, response)) {
        return false;
    }

    if (!response.errormessage().empty()) {
        BS_LOG(ERROR, "removeSnapshot request[%s] failed, path:[%s], error:[%s]", request.ShortDebugString().c_str(),
               _srcIndexAdminPath.c_str(), response.errormessage(0).c_str());
        return false;
    }
    BS_LOG(INFO, "removeSnapshot request[%s] done, server addr[%s]", request.ShortDebugString().c_str(),
           _bsChannel->getHostAddress().c_str());
    return true;
}

std::string CloneIndexTask::getGenerationPath(const std::string& indexRoot, const std::string& clusterName,
                                              uint32_t generationId)
{
    return indexRoot + "/" + clusterName + "/generation_" + std::to_string(generationId);
}

bool CloneIndexTask::checkAndGetClusterInfos(const GenerationInfo& generationInfo)
{
    if (!generationInfo.has_buildinfo()) {
        BS_LOG(ERROR, "missing buildinfo in generationInfo");
        return false;
    }
    const auto& buildInfo = generationInfo.buildinfo();
    if (buildInfo.clusterinfos_size() == 0) {
        BS_LOG(ERROR, "empty clusterInfos in buildInfo");
        return false;
    }

    size_t inTargetClusterCount = 0;

    for (int i = 0; i < buildInfo.clusterinfos_size(); ++i) {
        const auto& info = buildInfo.clusterinfos(i);
        if (!info.has_clustername()) {
            BS_LOG(ERROR, "missing clusterName in clusterInfo:[%d]", i);
            return false;
        }
        if (!info.has_partitioncount()) {
            BS_LOG(ERROR, "missing partitionCount in clusterInfo:[%d]", i);
            return false;
        }
        const auto& clusterName = info.clustername();
        auto it = _targetClusterMap.find(clusterName);
        if (it != _targetClusterMap.end()) {
            if (it->second.partitionCount != info.partitioncount()) {
                BS_LOG(ERROR, "partitionCount mismatch in src [%u] and target[%u]", info.partitioncount(),
                       it->second.partitionCount);
                return false;
            }
            inTargetClusterCount++;
        }
    }
    if (inTargetClusterCount != _targetClusterMap.size()) {
        BS_LOG(ERROR, "not all targetCluster found in gs info");
        return false;
    }
    return true;
}

bool CloneIndexTask::getSpecifiedVersionLocator(const GenerationInfo& generationInfo, const std::string& clusterName,
                                                uint32_t versionId, int64_t& indexLocator)
{
    if (!generationInfo.has_buildinfo()) {
        BS_LOG(ERROR, "generationInfo has no build info");
        return false;
    }

    const auto& buildInfo = generationInfo.buildinfo();
    for (int i = 0; i < buildInfo.clusterinfos_size(); ++i) {
        const auto& singleClusterInfo = buildInfo.clusterinfos(i);
        if (singleClusterInfo.has_clustername() && singleClusterInfo.clustername() == clusterName) {
            for (int j = 0; j < singleClusterInfo.checkpointinfos_size(); ++j) {
                const auto& checkpointInfo = singleClusterInfo.checkpointinfos(j);
                if (checkpointInfo.versionid() == versionId) {
                    if (_controlConfig.isIncProcessorExist(clusterName)) {
                        indexLocator = checkpointInfo.processorcheckpoint();
                    } else {
                        indexLocator = checkpointInfo.versiontimestamp();
                    }
                    return true;
                }
            }
        }
    }
    return false;
}

bool CloneIndexTask::getLastVersionId(const GenerationInfo& generationInfo, const std::string& clusterName,
                                      uint32_t& versionId)
{
    if (generationInfo.indexinfos_size() == 0) {
        BS_LOG(ERROR, "empty indexinfos in generationInfo");
        return false;
    }
    for (int i = 0; i < generationInfo.indexinfos_size(); ++i) {
        const auto& info = generationInfo.indexinfos(i);
        if (info.clustername() == clusterName) {
            versionId = info.indexversion();
            return true;
        }
    }
    return false;
}

}} // namespace build_service::task_base
