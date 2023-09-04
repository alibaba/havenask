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
#include "build_service/build_task/BuildTaskTarget.h"

#include "BuildTaskTarget.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "indexlib/framework/VersionCoord.h"
using namespace std;

namespace build_service { namespace build_task {
BS_LOG_SETUP(build_task, BuildTaskTarget);

BuildTaskTarget::BuildTaskTarget() {}

BuildTaskTarget::~BuildTaskTarget() {}

bool BuildTaskTarget::init(const config::TaskTarget& target)
{
    std::string taskTargetInfoStr;
    if (!target.getTargetDescription(config::BS_BUILD_TASK_TARGET, taskTargetInfoStr)) {
        BS_LOG(ERROR, "get build task target failed");
        return false;
    }
    try {
        autil::legacy::FromJsonString(_targetInfo, taskTargetInfoStr);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "deserialize task target failed");
        return false;
    }

    std::string dataDescStr;
    if (!target.getTargetDescription(config::DATA_DESCRIPTION_KEY, dataDescStr)) {
        BS_LOG(ERROR, "get data description failed");
        return false;
    }
    try {
        autil::legacy::FromJsonString(_dataDescription, dataDescStr);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "deserialize data description failed");
        return false;
    }
    if (!checkBuilderParam(_targetInfo)) {
        BS_LOG(ERROR, "check builder param failed");
        return false;
    }
    return true;
}

indexlibv2::framework::VersionCoord BuildTaskTarget::getVersionCoord() const
{
    auto buildMode = getBuildMode();
    if (buildMode & proto::PUBLISH) {
        return indexlibv2::framework::VersionCoord(getLatestPublishedVersion(), "");
    }
    assert(buildMode == proto::BUILD);
    if (_targetInfo.slaveVersionProgress.size() == 0) {
        return indexlibv2::framework::VersionCoord(getLatestPublishedVersion(), "");
    }
    auto& versionProgress = _targetInfo.slaveVersionProgress[0];
    return indexlibv2::framework::VersionCoord(versionProgress.versionId, versionProgress.fence);
}

bool BuildTaskTarget::getBatchMask(std::string* batchMask) const
{
    auto iter = _dataDescription.find(config::BATCH_MASK);
    if (iter == _dataDescription.end()) {
        return false;
    }
    *batchMask = iter->second;
    return true;
}

bool BuildTaskTarget::getSrcSignature(uint64_t* srcSignature) const
{
    auto iter = _dataDescription.find(config::SRC_SIGNATURE);
    if (iter == _dataDescription.end()) {
        return false;
    }
    return autil::StringUtil::fromString(iter->second, *srcSignature);
}

bool BuildTaskTarget::checkBuilderParam(const proto::BuildTaskTargetInfo& buildTaskInfo) const
{
    uint8_t buildMode = buildTaskInfo.buildMode;
    if (buildMode != proto::BUILD && buildMode != proto::PUBLISH && buildMode != proto::BUILD_PUBLISH) {
        BS_LOG(ERROR, "invalid build mode[%d]", buildMode);
        return false;
    }

    if (buildTaskInfo.buildStep != "full" && buildTaskInfo.buildStep != "incremental") {
        BS_LOG(ERROR, "un-expected buildStep [%s]", buildTaskInfo.buildStep.c_str());
        return false;
    }
    if (buildTaskInfo.buildMode != proto::PUBLISH) {
        if (buildTaskInfo.slaveVersionProgress.size() >= 2) {
            // size = 0, recover from latest published version; size = 1, recover from version progress
            BS_LOG(ERROR, "invalid version progress [%lu]", buildTaskInfo.slaveVersionProgress.size());
            return false;
        }
    }
    return true;
}

indexlibv2::versionid_t BuildTaskTarget::getAlignVersionId() const { return _targetInfo.alignVersionId; }

indexlibv2::versionid_t BuildTaskTarget::getLatestPublishedVersion() const
{
    return _targetInfo.latestPublishedVersion;
}

// proto::BuildMode BuildTaskTarget::getBuildMode() const { return _targetInfo.buildMode; }
// const std::string& BuildTaskTarget::getBuildStep() const { return _targetInfo.buildStep; }

}} // namespace build_service::build_task
