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
#include "build_service/admin/BuildTaskValidator.h"

#include <iosfwd>
#include <memory>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/config/BuildRuleConfig.h"
#include "build_service/config/ControlConfig.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/RangeUtil.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using build_service::util::IndexPathConstructor;

namespace build_service { namespace admin {

BS_LOG_SETUP(build_service, BuildTaskValidator);

const string BuildTaskValidator::BUILD_TASK_SIGNATURE_FILE = string("__build_task_signatue__");

BuildTaskValidator::BuildTaskValidator(const config::ResourceReaderPtr& resourceReader, const proto::BuildId& buildId,
                                       const string& indexRoot, const string& generationZkRoot,
                                       const proto::DataDescription& realTimeDataDesc)
    : _resourceReader(resourceReader)
    , _buildId(buildId)
    , _indexRoot(indexRoot)
    , _generationZkRoot(generationZkRoot)
    , _dataDescription(realTimeDataDesc)
{
}

BuildTaskValidator::~BuildTaskValidator() {}

bool BuildTaskValidator::ValidateAndWriteSignature(bool validateExistingIndex, string* errorMsg) const
{
    if (!_resourceReader) {
        *errorMsg = string("validate [") + _buildId.ShortDebugString() + string("] failed: resourceReader is NULL");
        BS_LOG(ERROR, "%s", errorMsg->c_str());
        return false;
    }

    config::ControlConfig controlConfig;
    if (!_resourceReader->getDataTableConfigWithJsonPath(_buildId.datatable(), "control_config", controlConfig)) {
        *errorMsg = string("get controlConfig for generation[") + _buildId.ShortDebugString() + string("] failed");
        return false;
    }
    vector<string> allClusters;
    if (!_resourceReader->getAllClusters(_buildId.datatable(), allClusters)) {
        *errorMsg = string("Get generation[") + _buildId.ShortDebugString() + string("] clusterNames failed");
        BS_LOG(ERROR, "%s", errorMsg->c_str());
        return false;
    }

    for (const string& cluster : allClusters) {
        if (controlConfig.isIncProcessorExist(cluster)) {
            BS_LOG(INFO,
                   "cluster [%s] controlConfig.isIncProcessorExist, skip validate task signature in generation[%s].",
                   cluster.c_str(), _buildId.ShortDebugString().c_str());
            continue;
        }
        string generationDir =
            IndexPathConstructor::constructGenerationPath(_indexRoot, cluster, _buildId.generationid());
        if (validateExistingIndex) {
            if (!ValidateRealtimeInfoAndPartitionCount(generationDir, cluster, errorMsg)) {
                return false;
            }
        }
        if (!ValidateAndWriteTaskSignature(generationDir, errorMsg)) {
            return false;
        }
    }
    return true;
}

set<string> BuildTaskValidator::GetPartitionDirs(uint32_t partitionCount)
{
    vector<proto::Range> ranges = util::RangeUtil::splitRange(RANGE_MIN, RANGE_MAX, partitionCount);
    set<string> partDirNames;
    for (const auto& range : ranges) {
        string expectPartName = IndexPathConstructor::PARTITION_PREFIX + autil::StringUtil::toString(range.from()) +
                                "_" + autil::StringUtil::toString(range.to()) + "/";
        partDirNames.emplace(expectPartName);
    }
    return partDirNames;
}

bool BuildTaskValidator::CleanTaskSignature(const config::ResourceReaderPtr& resourceReader,
                                            const std::string& indexRoot, const proto::BuildId& buildId,
                                            std::string* errorMsg)
{
    if (!resourceReader) {
        *errorMsg = string("clean signature for generation [") + buildId.ShortDebugString() +
                    string("] failed: resourceReader is NULL");
        BS_LOG(ERROR, "%s", errorMsg->c_str());
        return false;
    }

    config::ControlConfig controlConfig;
    if (!resourceReader->getDataTableConfigWithJsonPath(buildId.datatable(), "control_config", controlConfig)) {
        *errorMsg = string("get controlConfig for generation[") + buildId.ShortDebugString() + string("] failed");
        return false;
    }
    vector<string> allClusters;
    if (!resourceReader->getAllClusters(buildId.datatable(), allClusters)) {
        *errorMsg = string("Get generation[") + buildId.ShortDebugString() + string("] clusterNames failed");
        BS_LOG(ERROR, "%s", errorMsg->c_str());
        return false;
    }
    for (const string& cluster : allClusters) {
        if (controlConfig.isIncProcessorExist(cluster)) {
            BS_LOG(INFO,
                   "cluster[%s] controlConfig.isIncProcessorExist==true, skip clean task signature in generation[%s].",
                   cluster.c_str(), buildId.ShortDebugString().c_str());
            continue;
        }
        string clusterIndexRoot =
            IndexPathConstructor::constructGenerationPath(indexRoot, cluster, buildId.generationid());
        string signatureFilePath = fslib::util::FileUtil::joinFilePath(clusterIndexRoot, BUILD_TASK_SIGNATURE_FILE);
        if (!fslib::util::FileUtil::removeIfExist(signatureFilePath)) {
            BS_LOG(ERROR, "remove file[%s] in Task[%s] failed", signatureFilePath.c_str(),
                   buildId.ShortDebugString().c_str());
            return false;
        }
    }
    return true;
}

bool BuildTaskValidator::ValidatePartitionCount(const string& generationRoot, const string& clusterName,
                                                string* errorMsg) const
{
    config::BuildRuleConfig buildRuleConfig;
    if (!_resourceReader->getClusterConfigWithJsonPath(clusterName, "cluster_config.builder_rule_config",
                                                       buildRuleConfig)) {
        *errorMsg = string("parse cluster_config.builder_rule_config for [") + clusterName + string("] failed");
        BS_LOG(ERROR, "%s", errorMsg->c_str());
        return false;
    }
    uint32_t partitionCount = buildRuleConfig.partitionCount;
    vector<string> dirVec;
    if (!fslib::util::FileUtil::listDir(generationRoot, dirVec, false, false)) {
        *errorMsg = string("list dir[") + generationRoot + string("] failed");
        BS_LOG(ERROR, "%s", errorMsg->c_str());
        return false;
    }
    set<string> dirSet(dirVec.begin(), dirVec.end());
    set<string> expectedPartitionDirs = BuildTaskValidator::GetPartitionDirs(partitionCount);
    for (const auto& partDir : expectedPartitionDirs) {
        if (dirSet.count(partDir) != 1) {
            *errorMsg = string("validate partitionCount failed, partitionDir[") + partDir +
                        string("] not existed in ") + generationRoot;
            BS_LOG(WARN, "%s", errorMsg->c_str());
            return false;
        }
    }
    return true;
}

bool BuildTaskValidator::ValidateRealtimeInfoAndPartitionCount(const string& generationRoot, const string& clusterName,
                                                               string* errorMsg) const
{
    if (!ValidatePartitionCount(generationRoot, clusterName, errorMsg)) {
        return false;
    }
    if (!_dataDescription.kvMap.empty()) {
        if (!ValidateRealtimeInfo(_dataDescription, generationRoot, errorMsg)) {
            return false;
        }
    }
    return true;
}

bool BuildTaskValidator::ValidateRealtimeInfo(const proto::DataDescription& dataDescription,
                                              const string& generationRoot, string* errorMsg) const
{
    string realtimeInfoFile = fslib::util::FileUtil::joinFilePath(generationRoot, REALTIME_INFO_FILE_NAME);

    string fileContent;
    if (!fslib::util::FileUtil::readFile(realtimeInfoFile, fileContent)) {
        *errorMsg = string("read realtimeInfo file: [") + realtimeInfoFile + string("] failed for buildTask: [") +
                    _buildId.ShortDebugString() + string("]");
        BS_LOG(ERROR, "%s", errorMsg->c_str());
        return false;
    }

    KeyValueMap realtimeKvMap;
    if (!config::ResourceReader::parseRealtimeInfo(fileContent, realtimeKvMap)) {
        BS_LOG(ERROR, "parse realtimeInfo[%s] failed for buildId[%s]", realtimeInfoFile.c_str(),
               _buildId.ShortDebugString().c_str());
        return false;
    }

    for (auto it = dataDescription.begin(); it != dataDescription.end(); it++) {
        auto rit = realtimeKvMap.find(it->first);
        if (rit == realtimeKvMap.end()) {
            BS_LOG(ERROR, "key[%s] not found in realtimeInfo[%s]", it->first.c_str(), realtimeInfoFile.c_str());
            return false;
        }
        if (it->second != rit->second) {
            BS_LOG(ERROR, "value of [%s] missmatch in realtimeInfo[%s], lhs=[%s], rhs=[%s]", it->first.c_str(),
                   realtimeInfoFile.c_str(), it->second.c_str(), rit->second.c_str());
            return false;
        }
    }
    return true;
}

bool BuildTaskValidator::WriteTaskSignature(const string& signatureFilePath) const
{
    BuildTaskSignature taskSignature;
    taskSignature.zkRoot = _generationZkRoot;
    string content;
    try {
        content = autil::legacy::ToJsonString(taskSignature);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "fail to jsonize signature[%s] in Task[%s]", _generationZkRoot.c_str(),
               _buildId.ShortDebugString().c_str());
        return false;
    }
    if (!fslib::util::FileUtil::writeWithBak(signatureFilePath, content)) {
        BS_LOG(ERROR, "fail to write signature[%s] to file [%s] in Task[%s]", content.c_str(),
               signatureFilePath.c_str(), _buildId.ShortDebugString().c_str());
        return false;
    }
    return true;
}

bool BuildTaskValidator::ValidateTaskSignature(const string& signatureFilePath, bool* signatureExist,
                                               string* errorMsg) const
{
    bool existFlag = false;
    if (!fslib::util::FileUtil::isExist(signatureFilePath, existFlag)) {
        *errorMsg = string("fail to determine existence of file[") + signatureFilePath + string("] in Task: ") +
                    _buildId.ShortDebugString();
        BS_LOG(ERROR, "%s", errorMsg->c_str());
        return false;
    }
    *signatureExist = existFlag;
    if (!existFlag) {
        return true;
    }
    string content;
    if (!fslib::util::FileUtil::readFile(signatureFilePath, content)) {
        *errorMsg = string("fail to read content of file[") + signatureFilePath + string("] in Task: ") +
                    _buildId.ShortDebugString();
        BS_LOG(ERROR, "%s", errorMsg->c_str());
        return false;
    }
    BuildTaskSignature taskSignature;
    try {
        autil::legacy::FromJsonString(taskSignature, content);
    } catch (const autil::legacy::ExceptionBase& e) {
        *errorMsg = string("fail to jsonize content of file[") + signatureFilePath + string("] in Task: ") +
                    _buildId.ShortDebugString();
        BS_LOG(ERROR, "%s", errorMsg->c_str());
        return false;
    }
    if (taskSignature.zkRoot != _generationZkRoot) {
        *errorMsg = string("validate TaskSignature failed: signature's zkRoot[") + taskSignature.zkRoot +
                    string("] conflicts with current zkRoot[") + _generationZkRoot + string("].");
        BS_LOG(ERROR, "%s", errorMsg->c_str());
        return false;
    }
    return true;
}

bool BuildTaskValidator::ValidateAndWriteTaskSignature(const string& generationRoot, string* errorMsg) const
{
    string signatureFilePath = fslib::util::FileUtil::joinFilePath(generationRoot, BUILD_TASK_SIGNATURE_FILE);
    bool signatureExist = false;
    if (!ValidateTaskSignature(signatureFilePath, &signatureExist, errorMsg)) {
        return false;
    }
    if (!signatureExist) {
        if (!WriteTaskSignature(signatureFilePath)) {
            *errorMsg = string("fail to write signature file[") + signatureFilePath + string("] in Task: ") +
                        _buildId.ShortDebugString();
            BS_LOG(ERROR, "%s", errorMsg->c_str());
            return false;
        }
        return true;
    }
    return true;
}

bool BuildTaskValidator::TEST_prepareFakeIndex() const
{
    vector<string> allClusters;
    if (!_resourceReader->getAllClusters(_buildId.datatable(), allClusters)) {
        BS_LOG(ERROR, "get clusters failed for [%s]", _buildId.ShortDebugString().c_str());
        return false;
    }
    for (const string& clusterName : allClusters) {
        config::BuildRuleConfig buildRuleConfig;
        if (!_resourceReader->getClusterConfigWithJsonPath(clusterName, "cluster_config.builder_rule_config",
                                                           buildRuleConfig)) {
            BS_LOG(ERROR, "parse cluster_config.builder_rule_config for [%s] failed",
                   _buildId.ShortDebugString().c_str());
            return false;
        }
        uint32_t partitionCount = buildRuleConfig.partitionCount;

        string generationDir =
            IndexPathConstructor::constructGenerationPath(_indexRoot, clusterName, _buildId.generationid());
        bool isExist = false;
        if (!fslib::util::FileUtil::isExist(generationDir, isExist)) {
            return false;
        }
        if (!isExist) {
            if (!fslib::util::FileUtil::mkDir(generationDir, true)) {
                BS_LOG(ERROR, "make generation dir[%s] failed", generationDir.c_str());
                return false;
            }
        }
        set<string> partDirs = GetPartitionDirs(partitionCount);
        for (const auto& partDir : partDirs) {
            if (!fslib::util::FileUtil::mkDir(fslib::util::FileUtil::joinFilePath(generationDir, partDir), false)) {
                return false;
            }
        }
    }
    return true;
}

}} // namespace build_service::admin
