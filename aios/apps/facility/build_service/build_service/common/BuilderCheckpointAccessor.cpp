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
#include "build_service/common/BuilderCheckpointAccessor.h"

#include <exception>
#include <iosfwd>
#include <memory>
#include <utility>

#include "alog/Logger.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/common/Checkpoint.h"
#include "build_service/common/CheckpointAccessor.h"
#include "build_service/common/IndexCheckpointFormatter.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/proto/BuildTaskCurrentInfo.h"
#include "indexlib/framework/VersionMeta.h"

using namespace std;

namespace build_service { namespace common {

BS_LOG_SETUP(common, BuilderCheckpointAccessor);

BuilderCheckpointAccessor::BuilderCheckpointAccessor(const CheckpointAccessorPtr& accessor) : _accessor(accessor) {}

BuilderCheckpointAccessor::~BuilderCheckpointAccessor() {}

bool BuilderCheckpointAccessor::getLatestCheckpoint(const string& clusterName,
                                                    proto::BuilderCheckpoint& builderCheckpoint, string& ckpValue)
{
    string builderCheckpointId = IndexCheckpointFormatter::getBuilderCheckpointId(clusterName);
    string checkpointName;
    if (!_accessor->getLatestCheckpoint(builderCheckpointId, checkpointName, ckpValue)) {
        return false;
    }
    IndexCheckpointFormatter::decodeBuilderCheckpoint(ckpValue, builderCheckpoint);
    return true;
}

bool BuilderCheckpointAccessor::getLatestCheckpoint(const string& clusterName,
                                                    proto::BuilderCheckpoint& builderCheckpoint)
{
    string ckpValue;
    return getLatestCheckpoint(clusterName, builderCheckpoint, ckpValue);
}

void BuilderCheckpointAccessor::addCheckpoint(const string& clusterName, const proto::BuilderCheckpoint& checkpointInfo,
                                              int32_t keepCount)
{
    string ckpId = IndexCheckpointFormatter::getBuilderCheckpointId(clusterName);
    string ckpName =
        IndexCheckpointFormatter::getBuilderCheckpointName(checkpointInfo.schemaversion(), checkpointInfo.versionid());
    string ckpValue = IndexCheckpointFormatter::encodeBuilderCheckpoint(checkpointInfo);
    vector<pair<string, string>> removeCheckpoints;
    _accessor->addCheckpoint(ckpId, ckpName, ckpValue);
    if (keepCount > 0) {
        _accessor->updateReservedCheckpoint(ckpId, keepCount, removeCheckpoints);
    }
}

void BuilderCheckpointAccessor::addMasterCheckpoint(const std::string& clusterName, const proto::Range& range,
                                                    versionid_t versionId, const std::string& checkpoint)
{
    string ckpId = IndexCheckpointFormatter::getMasterBuilderCheckpointId(clusterName, range);
    string ckpName = IndexCheckpointFormatter::getMasterBuilderCheckpointName(versionId);
    vector<pair<string, string>> removeCheckpoints;
    _accessor->addOrUpdateCheckpoint(ckpId, ckpName, checkpoint);
    _accessor->updateReservedCheckpoint(ckpId, 1, removeCheckpoints);
}

bool BuilderCheckpointAccessor::getMasterCheckpoint(const std::string& clusterName, const proto::Range& range,
                                                    std::string& checkpoint)
{
    string ckpId = IndexCheckpointFormatter::getMasterBuilderCheckpointId(clusterName, range);
    string checkpointName;
    return _accessor->getLatestCheckpoint(ckpId, checkpointName, checkpoint);
}

void BuilderCheckpointAccessor::addSlaveCheckpoint(const std::string& clusterName, const proto::Range& range,
                                                   versionid_t versionId, const std::string& checkpoint)
{
    string ckpId = IndexCheckpointFormatter::getSlaveBuilderCheckpointId(clusterName, range);
    string ckpName = IndexCheckpointFormatter::getSlaveBuilderCheckpointName(versionId);
    vector<pair<string, string>> removeCheckpoints;
    _accessor->addOrUpdateCheckpoint(ckpId, ckpName, checkpoint);
    _accessor->updateReservedCheckpoint(ckpId, 1, removeCheckpoints);
}

bool BuilderCheckpointAccessor::getSlaveCheckpoint(const std::string& clusterName, const proto::Range& range,
                                                   std::string& checkpoint)
{
    string ckpId = IndexCheckpointFormatter::getSlaveBuilderCheckpointId(clusterName, range);
    string checkpointName;
    return _accessor->getLatestCheckpoint(ckpId, checkpointName, checkpoint);
}

void BuilderCheckpointAccessor::clearCheckpoint(const std::string& clusterName)
{
    string ckpId = IndexCheckpointFormatter::getBuilderCheckpointId(clusterName);
    vector<pair<string, string>> removeCheckpoints;
    _accessor->updateReservedCheckpoint(ckpId, 0, removeCheckpoints);
}

void BuilderCheckpointAccessor::clearMasterCheckpoint(const std::string& clusterName, const proto::Range& range)
{
    string ckpId = IndexCheckpointFormatter::getMasterBuilderCheckpointId(clusterName, range);
    vector<pair<string, string>> removeCheckpoints;
    _accessor->updateReservedCheckpoint(ckpId, 0, removeCheckpoints);
}

void BuilderCheckpointAccessor::clearSlaveCheckpoint(const std::string& clusterName, const proto::Range& range)
{
    string ckpId = IndexCheckpointFormatter::getSlaveBuilderCheckpointId(clusterName, range);
    vector<pair<string, string>> removeCheckpoints;
    _accessor->updateReservedCheckpoint(ckpId, 0, removeCheckpoints);
}

bool BuilderCheckpointAccessor::createSavepoint(const string& applyRole, const string& clusterName,
                                                const proto::BuilderCheckpoint& checkpointInfo)
{
    string ckpId = IndexCheckpointFormatter::getBuilderCheckpointId(clusterName);
    string ckpName =
        IndexCheckpointFormatter::getBuilderCheckpointName(checkpointInfo.schemaversion(), checkpointInfo.versionid());
    string errorMsg;
    return _accessor->createSavepoint(applyRole, ckpId, ckpName, errorMsg);
}

bool BuilderCheckpointAccessor::removeSavepoint(const string& applyRole, const string& clusterName,
                                                const proto::BuilderCheckpoint& checkpointInfo)
{
    string ckpId = IndexCheckpointFormatter::getBuilderCheckpointId(clusterName);
    string ckpName =
        IndexCheckpointFormatter::getBuilderCheckpointName(checkpointInfo.schemaversion(), checkpointInfo.versionid());
    string errorMsg;
    return _accessor->removeSavepoint(applyRole, ckpId, ckpName, errorMsg);
}

set<indexlibv2::framework::VersionCoord>
BuilderCheckpointAccessor::getSlaveReservedVersions(const std::string& clusterName,
                                                    const std::vector<proto::Range>& ranges)
{
    set<indexlibv2::framework::VersionCoord> ret;
    for (const auto& range : ranges) {
        string ckpId = IndexCheckpointFormatter::getSlaveBuilderCheckpointId(clusterName, range);
        vector<pair<string, string>> checkpoints;
        _accessor->listCheckpoints(ckpId, checkpoints);
        for (const auto& ckp : checkpoints) {
            try {
                BuildCheckpoint checkpoint;
                autil::legacy::FromJsonString(checkpoint, ckp.second);
                const auto& committedVersion = checkpoint.buildInfo.commitedVersion;
                ret.emplace(committedVersion.versionMeta.GetVersionId(), committedVersion.versionMeta.GetFenceName());
            } catch (const autil::legacy::ExceptionBase& e) {
                BS_LOG(ERROR, "get slave reserved versions caught exception, %s", e.what());
            } catch (const std::exception& e) {
                BS_LOG(ERROR, "get slave reserved versions caught exception, %s", e.what());
            } catch (...) {
                BS_LOG(ERROR, "get slave reserved versions caught unknown exception");
            }
        }
    }
    return ret;
}

set<versionid_t> BuilderCheckpointAccessor::getReservedVersions(const std::string& clusterName)
{
    set<versionid_t> ret;
    string ckpId = IndexCheckpointFormatter::getBuilderCheckpointId(clusterName);
    vector<pair<string, string>> checkpoints;
    _accessor->listCheckpoints(ckpId, checkpoints);
    for (auto& ckp : checkpoints) {
        versionid_t versionId = indexlib::INVALID_VERSIONID;
        int64_t schemaId = config::INVALID_SCHEMAVERSION;
        if (IndexCheckpointFormatter::decodeBuilderCheckpointName(ckp.first, schemaId, versionId) &&
            versionId != indexlib::INVALID_VERSIONID) {
            ret.insert(versionId);
        }
    }
    return ret;
}

}} // namespace build_service::common
