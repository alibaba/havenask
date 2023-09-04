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
#include "build_service/admin/ProcessorCheckpointAccessor.h"

#include "autil/StringUtil.h"
#include "build_service/admin/CheckpointCreator.h"
#include "build_service/admin/ProcessorCheckpointFormatter.h"
#include "build_service/common/IndexCheckpointAccessor.h"

using namespace std;
using autil::StringUtil;
using build_service::common::IndexCheckpointAccessor;
using build_service::common::IndexCheckpointAccessorPtr;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ProcessorCheckpointAccessor);

ProcessorCheckpointAccessor::~ProcessorCheckpointAccessor() {}

bool ProcessorCheckpointAccessor::registCheckpointInfo(const std::string& cluster, const std::string& topicId,
                                                       const std::string& preStage, const std::string& stage,
                                                       bool isLastStage)
{
    string innerStage = stage;
    if (innerStage.empty()) {
        innerStage = string("EmptyStage_") + topicId;
        isLastStage = true;
    }
    auto key = make_pair(cluster, innerStage);
    auto iter = _checkpointInfos.find(key);
    if (iter == _checkpointInfos.end()) {
        ClusterCheckpointInfo info(innerStage, preStage, topicId, isLastStage);
        _checkpointInfos[key] = info;
        return true;
    }
    ClusterCheckpointInfo* clusterInfo = &(iter->second);
    if (topicId != clusterInfo->topicId || preStage != clusterInfo->preStage ||
        isLastStage != clusterInfo->isLastStage) {
        BS_LOG(ERROR,
               "conflict checkpoint info, original info"
               " [clusterName:%s, topicId:%s, preStage:%s, stage:%s, isLastStage: %d],"
               " current info [clusterName:%s, topicId:%s, preStage:%s, stage:%s, isLastStage: %d]",
               cluster.c_str(), clusterInfo->topicId.c_str(), clusterInfo->preStage.c_str(), clusterInfo->stage.c_str(),
               clusterInfo->isLastStage, cluster.c_str(), topicId.c_str(), preStage.c_str(), stage.c_str(),
               isLastStage);
        return false;
    }
    return true;
}

void ProcessorCheckpointAccessor::addCheckpoint(const std::string& cluster, const std::string& topicId,
                                                const std::string& stage, const common::Locator& locator)
{
    string innerStage = stage;
    if (innerStage.empty()) {
        innerStage = string("EmptyStage_") + topicId;
    }

    CheckpointInfo checkpointInfo(locator);
    auto key = make_pair(cluster, innerStage);
    auto iter = _checkpointInfos.find(key);
    if (iter == _checkpointInfos.end()) {
        BS_LOG(ERROR, "checkpoint info not regist: [cluster:%s, topicId:%s, stage:%s]", cluster.c_str(),
               topicId.c_str(), stage.c_str());
        return;
    }
    ClusterCheckpointInfo& clusterInfo = iter->second;
    clusterInfo.checkpointList.push_back(checkpointInfo);
    if (clusterInfo.checkpointList.size() > 2000) {
        clusterInfo.checkpointList.pop_front();
    }
    commitCheckpoint(cluster, clusterInfo);
    updateCheckpoint(cluster, topicId, locator);
}

void ProcessorCheckpointAccessor::commitCheckpoint(const std::string& cluster, ClusterCheckpointInfo& clusterInfo)
{
    bool isLastStage = clusterInfo.isLastStage;
    auto checkpointInfo = &(clusterInfo.checkpointList.back());
    auto preStage = clusterInfo.preStage;
    common::Locator srcLocator;

    // find srcLocator for cluserInfo.topicId
    while (true) {
        if (preStage.empty()) {
            // no preStage, current checkpointInfo is src checkpoint
            srcLocator = checkpointInfo->locator;
            break;
        }
        auto key = make_pair(cluster, preStage);
        auto iter = _checkpointInfos.find(key);
        if (iter == _checkpointInfos.end()) {
            // no pre cluster checkpoint info, return
            return;
        }
        auto& tmpClusterInfo = iter->second;
        preStage = tmpClusterInfo.preStage;
        int64_t committedCheckpointIdx = -1;
        for (size_t i = 0; i < tmpClusterInfo.checkpointList.size(); i++) {
            auto& preCheckpoint = tmpClusterInfo.checkpointList[i];
            if (preCheckpoint.ts < checkpointInfo->locator.GetOffset().first) {
                // current cluster checkpoint offset bigger than preCheckpoint ts
                // then preCheckpoint locator is already in current topicId
                committedCheckpointIdx = i;
                continue;
            }
            break;
        }
        if (committedCheckpointIdx == -1) {
            // can not commit, no smaller pre checkpoint
            return;
        }
        // use tmpClusterInfo checkpoint to find pre checkpoint
        checkpointInfo = &tmpClusterInfo.checkpointList[committedCheckpointIdx];

        // clean useless in mem checkpoint
        if (isLastStage) {
            for (int64_t i = 0; i < committedCheckpointIdx; i++) {
                tmpClusterInfo.checkpointList.pop_front();
            }
        }
    }
    updateCommitCheckpoint(cluster, clusterInfo.topicId, srcLocator);
    if (isLastStage) {
        clusterInfo.checkpointList.clear();
    }
}

void ProcessorCheckpointAccessor::updateCommitCheckpoint(const std::string& clusterName, const std::string& topicId,
                                                         const common::Locator& locator)
{
    string checkpointName = ProcessorCheckpointFormatter::getProcessorCommitedCheckpointName(clusterName, topicId);
    string locatorString = StringUtil::strToHexStr(locator.Serialize());
    // std::cout << "update commit processor checkpoint: " << checkpointName << ", " << locator.getOffset() <<
    // std::endl;
    if (!_checkpointAccessor->updateCheckpoint(BS_PROCESSOR_CHECKPOINT_ID, checkpointName, locatorString)) {
        _checkpointAccessor->addCheckpoint(BS_PROCESSOR_CHECKPOINT_ID, checkpointName, locatorString);
    }
}

void ProcessorCheckpointAccessor::updateCheckpoint(const std::string& cluster, const std::string& topicId,
                                                   const common::Locator& locator)
{
    string checkpointName = ProcessorCheckpointFormatter::getProcessorCheckpointName(cluster, topicId);
    string locatorString = StringUtil::strToHexStr(locator.Serialize());
    // std::cout << "update processor checkpoint: " << checkpointName << ", " << locatorString << "," <<
    // locator.getOffset() << std::endl;
    if (!_checkpointAccessor->updateCheckpoint(BS_PROCESSOR_CHECKPOINT_ID, checkpointName, locatorString)) {
        _checkpointAccessor->addCheckpoint(BS_PROCESSOR_CHECKPOINT_ID, checkpointName, locatorString);
    }
}

int32_t ProcessorCheckpointAccessor::createMaxProcessorTaskId()
{
    int32_t id = 0;
    string maxId;
    if (!_checkpointAccessor->getCheckpoint(BS_PROCESSOR_MAX_ID, BS_PROCESSOR_MAX_ID, maxId)) {
        id = 0;
        _checkpointAccessor->addCheckpoint(BS_PROCESSOR_MAX_ID, BS_PROCESSOR_MAX_ID, "0");
        return 0;
    }
    StringUtil::fromString(maxId, id);
    id++;
    _checkpointAccessor->updateCheckpoint(BS_PROCESSOR_MAX_ID, BS_PROCESSOR_MAX_ID, StringUtil::toString(id));
    return id;
}

bool ProcessorCheckpointAccessor::getCheckpoint(const std::string& cluster, const std::string& topicId,
                                                common::Locator* locator)
{
    string checkpointName = ProcessorCheckpointFormatter::getProcessorCheckpointName(cluster, topicId);
    return getCheckpoint(checkpointName, locator);
}

bool ProcessorCheckpointAccessor::getCheckpoint(const std::string& checkpointName, common::Locator* locator)
{
    string checkPointValue;
    if (_checkpointAccessor->getCheckpoint(BS_PROCESSOR_CHECKPOINT_ID, checkpointName, checkPointValue)) {
        string locatorString = StringUtil::hexStrToStr(checkPointValue);
        locator->Deserialize(locatorString);
        return true;
    }
    return false;
}

bool ProcessorCheckpointAccessor::getCheckpointFromIndex(const std::string& cluster, int64_t schemaId,
                                                         common::Locator* locator)
{
    IndexCheckpointAccessorPtr indexAccessor(new IndexCheckpointAccessor(_checkpointAccessor));
    proto::CheckpointInfo checkpoint;
    string checkpointName;
    if (indexAccessor->getLatestIndexCheckpoint(cluster, schemaId, checkpoint, checkpointName)) {
        locator->SetSrc(checkpoint.processordatasourceidx());
        locator->SetOffset({checkpoint.processorcheckpoint(), 0});
        return true;
    }

    return false;
}

bool ProcessorCheckpointAccessor::getCheckpointFromIndex(const std::string& cluster, common::Locator* locator)
{
    IndexCheckpointAccessorPtr indexAccessor(new IndexCheckpointAccessor(_checkpointAccessor));
    string checkpointName;
    proto::CheckpointInfo checkpoint;
    if (indexAccessor->getLatestIndexCheckpoint(cluster, checkpoint, checkpointName)) {
        locator->SetSrc(checkpoint.processordatasourceidx());
        locator->SetOffset({checkpoint.processorcheckpoint(), 0});
        return true;
    }
    return false;
}

bool ProcessorCheckpointAccessor::getCommitedCheckpoint(const std::string& cluster, const std::string& topicId,
                                                        common::Locator* locator)
{
    string checkpointName = ProcessorCheckpointFormatter::getProcessorCommitedCheckpointName(cluster, topicId);
    string checkPointValue;
    if (_checkpointAccessor->getCheckpoint(BS_PROCESSOR_CHECKPOINT_ID, checkpointName, checkPointValue)) {
        string locatorString = StringUtil::hexStrToStr(checkPointValue);
        locator->Deserialize(locatorString);
        // std::cout << "get committed checkpoint: " << checkpointName << "," << locator->getOffset() << std::endl;
        return true;
    }
    return false;
}

}} // namespace build_service::admin
