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
#include "build_service/build_task/BuildTaskCurrent.h"

#include <iosfwd>

#include "autil/legacy/legacy_jsonizable.h"

using namespace std;

namespace build_service { namespace build_task {
BS_LOG_SETUP(build_task, BuildTaskCurrent);

BuildTaskCurrent::BuildTaskCurrent() {}

BuildTaskCurrent::~BuildTaskCurrent() {}

void BuildTaskCurrent::updateCurrent(const proto::VersionInfo& commitedVersionInfo,
                                     indexlibv2::versionid_t alignFailedVersionId, bool buildFinished,
                                     const std::shared_ptr<BuildTaskTarget>& target)
{
    indexlibv2::versionid_t commitedVersionId = commitedVersionInfo.versionMeta.GetVersionId();
    if (commitedVersionId != indexlibv2::INVALID_VERSIONID) {
        _buildCurrentInfo.commitedVersion = commitedVersionInfo;
    }
    if (alignFailedVersionId != indexlibv2::INVALID_VERSIONID) {
        _buildCurrentInfo.lastAlignFailedVersionId = alignFailedVersionId;
    }
    _buildFinished = buildFinished;
    auto alignVersionId = indexlibv2::INVALID_VERSIONID;
    if (target && target->getAlignVersionId() != _buildCurrentInfo.lastAlignedVersion.versionMeta.GetVersionId()) {
        alignVersionId = target->getAlignVersionId();
    }
    bool alignSuccess = false;
    // update align version info
    if (alignVersionId == indexlibv2::INVALID_VERSIONID) {
        alignSuccess = true;
    } else {
        if (alignVersionId == commitedVersionId && alignVersionId != alignFailedVersionId &&
            alignVersionId != _buildCurrentInfo.lastAlignFailedVersionId) {
            _buildCurrentInfo.lastAlignedVersion = commitedVersionInfo;
            alignSuccess = true;
        }
    }
    if (!_buildFinished) {
        return;
    }
    if (alignSuccess && target) {
        std::lock_guard<std::mutex> lock(_targetMutex);
        _reachedTarget = target;
    }
}

bool BuildTaskCurrent::reachedTarget(const std::shared_ptr<BuildTaskTarget>& target)
{
    std::lock_guard<std::mutex> lock(_targetMutex);
    if (!_reachedTarget) {
        return false;
    }
    return *_reachedTarget == *target;
}

std::string BuildTaskCurrent::getTaskStatus() const { return autil::legacy::ToJsonString(_buildCurrentInfo); }

}} // namespace build_service::build_task
