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
#include "build_service/common/IndexCheckpointAccessor.h"

#include "build_service/common/BuilderCheckpointAccessor.h"
#include "build_service/common/CheckpointAccessor.h"
#include "build_service/common/IndexCheckpointFormatter.h"

using namespace std;
using namespace autil;

namespace build_service { namespace common {
BS_LOG_SETUP(common, IndexCheckpointAccessor);

IndexCheckpointAccessor::IndexCheckpointAccessor(const CheckpointAccessorPtr& accessor) : _accessor(accessor)
{
    assert(accessor);
}

IndexCheckpointAccessor::~IndexCheckpointAccessor() {}

bool IndexCheckpointAccessor::getIndexCheckpoint(const string& clusterName, versionid_t versionId,
                                                 proto::CheckpointInfo& checkpointInfo)
{
    string ckpId = IndexCheckpointFormatter::getIndexCheckpointId(true, clusterName);
    string ckpName = IndexCheckpointFormatter::encodeCheckpointName(versionId);
    string checkpoint;
    if (!_accessor->getCheckpoint(ckpId, ckpName, checkpoint)) {
        return false;
    }
    return IndexCheckpointFormatter::decodeCheckpoint(checkpoint, checkpointInfo);
}

bool IndexCheckpointAccessor::getLatestIndexCheckpoint(const string& clusterName, proto::CheckpointInfo& checkpointInfo,
                                                       string& checkpointName)
{
    string checkpointId = IndexCheckpointFormatter::getIndexCheckpointId(true, clusterName);
    string checkpointValue;
    if (_accessor->getLatestCheckpoint(checkpointId, checkpointName, checkpointValue)) {
        return IndexCheckpointFormatter::decodeCheckpoint(checkpointValue, checkpointInfo);
    }
    return false;
}

bool IndexCheckpointAccessor::getLatestIndexCheckpoint(const std::string& clusterName, int64_t schemaId,
                                                       proto::CheckpointInfo& checkpointInfo,
                                                       std::string& checkpointName)
{
    string ckpId = IndexCheckpointFormatter::getIndexCheckpointId(true, clusterName);
    vector<pair<string, string>> checkpoints;
    _accessor->listCheckpoints(ckpId, checkpoints);
    for (int64_t i = checkpoints.size() - 1; i > 0; i--) {
        if (IndexCheckpointFormatter::decodeCheckpoint(checkpoints[i].second, checkpointInfo)) {
            if (checkpointInfo.schemaid() == schemaId) {
                return true;
            }
        }
    }
    return false;
}

void IndexCheckpointAccessor::addIndexCheckpoint(const string& clusterName, versionid_t targetVersion,
                                                 int32_t keepCount, proto::CheckpointInfo& checkpointInfo)
{
    string ckpId = IndexCheckpointFormatter::getIndexCheckpointId(true, clusterName);
    string ckpName = IndexCheckpointFormatter::encodeCheckpointName(targetVersion);
    string checkpointValue = IndexCheckpointFormatter::encodeCheckpoint(checkpointInfo);
    std::vector<std::pair<std::string, std::string>> removeCheckpoints;
    _accessor->addCheckpoint(ckpId, ckpName, checkpointValue);
    if (keepCount > 0) {
        _accessor->updateReservedCheckpoint(ckpId, keepCount, removeCheckpoints);
    }
}

bool IndexCheckpointAccessor::createIndexSavepoint(const std::string& applyRole, const std::string& clusterName,
                                                   versionid_t targetVersion)
{
    proto::CheckpointInfo checkpointInfo;
    std::string errorMsg;
    return createIndexSavepoint(applyRole, clusterName, targetVersion, "", checkpointInfo, errorMsg);
}

bool IndexCheckpointAccessor::createIndexSavepoint(const std::string& applyRole, const std::string& clusterName,
                                                   versionid_t targetVersion, const std::string& comment,
                                                   proto::CheckpointInfo& checkpointInfo, std::string& errorMsg)
{
    std::string ckpId = IndexCheckpointFormatter::getIndexCheckpointId(true, clusterName);
    std::string ckpName = IndexCheckpointFormatter::encodeCheckpointName(targetVersion);
    if (!_accessor->createSavepoint(applyRole, ckpId, ckpName, errorMsg, comment)) {
        BS_LOG(ERROR, "create savepoint failed, errMsg[%s], ckpId[%s], ckpName[%s]", errorMsg.c_str(), ckpId.c_str(),
               ckpName.c_str());
        return false;
    }
    std::string checkpointStr;
    if (!_accessor->getCheckpoint(ckpId, ckpName, checkpointStr)) {
        errorMsg = "get savepoint failed, ckpId[" + ckpId + "], ckpName[" + ckpName + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (!IndexCheckpointFormatter::decodeCheckpoint(checkpointStr, checkpointInfo)) {
        errorMsg = "decode savepoint failed, ckpId[" + ckpId + "], ckpName[" + ckpName + "], checkpointStr[" +
                   checkpointStr + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool IndexCheckpointAccessor::removeIndexSavepoint(const std::string& applyRole, const std::string& clusterName,
                                                   versionid_t targetVersion)
{
    std::string errorMsg;
    return removeIndexSavepoint(applyRole, clusterName, targetVersion, errorMsg);
}

bool IndexCheckpointAccessor::removeIndexSavepoint(const std::string& applyRole, const std::string& clusterName,
                                                   versionid_t targetVersion, std::string& errorMsg)
{
    std::string ckpId = IndexCheckpointFormatter::getIndexCheckpointId(true, clusterName);
    std::string ckpName = IndexCheckpointFormatter::encodeCheckpointName(targetVersion);
    return _accessor->removeSavepoint(applyRole, ckpId, ckpName, errorMsg);
}

void IndexCheckpointAccessor::clearIndexCheckpoint(const string& clusterName, versionid_t targetVersion)
{
    string ckpId = IndexCheckpointFormatter::getIndexCheckpointId(true, clusterName);
    vector<pair<string, string>> checkpoints;
    _accessor->listCheckpoints(ckpId, checkpoints);
    set<string> needToBeRemoved;
    for (auto& ckp : checkpoints) {
        versionid_t versionId = INVALID_VERSION;
        if (!StringUtil::fromString(ckp.first, versionId)) {
            needToBeRemoved.insert(ckp.first);
            continue;
        }
        if (versionId <= targetVersion) {
            needToBeRemoved.insert(ckp.first);
            continue;
        }
    }
    _accessor->cleanCheckpoints(ckpId, needToBeRemoved);
}

bool IndexCheckpointAccessor::hasFullIndexInfo(const std::string& clusterName)
{
    string ckpId = IndexCheckpointFormatter::getIndexInfoId(true, clusterName);
    vector<pair<string, string>> checkpoints;
    _accessor->listCheckpoints(ckpId, checkpoints);
    return checkpoints.size() > 0;
}

void IndexCheckpointAccessor::updateIndexInfo(bool isFullIndex, const string& clusterName,
                                              const ::google::protobuf::RepeatedPtrField<proto::IndexInfo>& indexInfos)
{
    string ckpId = IndexCheckpointFormatter::getIndexInfoId(isFullIndex, clusterName);
    string ckpName = ckpId;
    std::vector<proto::JsonizableProtobuf<proto::IndexInfo>> jsonizableIndexInfos;
    jsonizableIndexInfos.reserve(indexInfos.size());
    for (const auto& indexInfo : indexInfos) {
        jsonizableIndexInfos.push_back(proto::JsonizableProtobuf<proto::IndexInfo>(indexInfo));
    }
    string checkpointValue = IndexCheckpointFormatter::encodeIndexInfo(jsonizableIndexInfos);

    if (!_accessor->updateCheckpoint(ckpId, ckpName, checkpointValue)) {
        _accessor->addCheckpoint(ckpId, ckpName, checkpointValue);
    }
}

bool IndexCheckpointAccessor::getIndexInfo(bool isFull, const string& clusterName,
                                           ::google::protobuf::RepeatedPtrField<proto::IndexInfo>& indexInfos)
{
    string ckpId = IndexCheckpointFormatter::getIndexInfoId(isFull, clusterName);
    string ckpName = ckpId;
    string checkpointValue;
    if (!_accessor->getCheckpoint(ckpId, ckpName, checkpointValue, false)) {
        return false;
    }
    IndexCheckpointFormatter::decodeIndexInfo(checkpointValue, indexInfos);
    return true;
}

bool IndexCheckpointAccessor::isSavepoint(const std::string& clusterName, versionid_t version)
{
    string ckpId = IndexCheckpointFormatter::getIndexCheckpointId(true, clusterName);
    string ckpName = IndexCheckpointFormatter::encodeCheckpointName(version);
    std::string comment;
    return _accessor->isSavepoint(ckpId, ckpName, &comment);
}

set<versionid_t> IndexCheckpointAccessor::getReservedVersions(const string& clusterName)
{
    set<versionid_t> ret;
    string ckpId = IndexCheckpointFormatter::getIndexCheckpointId(true, clusterName);
    vector<pair<string, string>> checkpoints;
    _accessor->listCheckpoints(ckpId, checkpoints);
    for (auto& ckp : checkpoints) {
        versionid_t versionId = INVALID_VERSION;
        if (StringUtil::fromString(ckp.first, versionId) && versionId != INVALID_VERSION) {
            ret.insert(versionId);
        }
    }
    return ret;
}

set<versionid_t> IndexCheckpointAccessor::getReservedVersions(const string& clusterName,
                                                              const IndexCheckpointAccessorPtr& indexCkpAccessor,
                                                              const BuilderCheckpointAccessorPtr& builderCkpAccessor)
{
    set<versionid_t> versions = indexCkpAccessor->getReservedVersions(clusterName);
    set<versionid_t> buildVersions = builderCkpAccessor->getReservedVersions(clusterName);
    versions.insert(buildVersions.begin(), buildVersions.end());
    return versions;
}

bool IndexCheckpointAccessor::listCheckpoint(const std::string& clusterName, bool savepointOnly, uint32_t limit,
                                             std::vector<proto::CheckpointInfo>* checkpointInfos) const
{
    std::string ckpId = IndexCheckpointFormatter::getIndexCheckpointId(true, clusterName);
    std::vector<std::pair<std::string, std::string>> ckpts;
    _accessor->listCheckpoints(ckpId, ckpts);
    for (auto iter = ckpts.rbegin(); iter != ckpts.rend(); iter++) {
        const auto& [ckptKey, ckptStr] = *iter;
        if (checkpointInfos->size() == limit) {
            break;
        }
        proto::CheckpointInfo checkpointInfo;
        if (!IndexCheckpointFormatter::decodeCheckpoint(ckptStr, checkpointInfo)) {
            return false;
        }
        std::string comment;
        if (savepointOnly == false || _accessor->isSavepoint(ckpId, ckptKey, &comment)) {
            checkpointInfos->push_back(checkpointInfo);
        }
    }
    return true;
}

bool IndexCheckpointAccessor::getSchemaChangedStandard(const std::string& clusterName, std::string& changedSchema)
{
    string ckpShemaPatchId = IndexCheckpointFormatter::getSchemaPatch(clusterName);
    return _accessor->getCheckpoint(ckpShemaPatchId, BS_CKP_SCHEMA_PATCH, changedSchema, false);
}

}} // namespace build_service::common
