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
#pragma once

#include <memory>
#include <mutex>
#include <string>

#include "build_service/build_task/BuildTaskTarget.h"
#include "build_service/common_define.h"
#include "build_service/proto/BuildTaskCurrentInfo.h"
#include "build_service/util/Log.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/VersionMeta.h"

namespace build_service { namespace build_task {

class BuildTaskCurrent
{
public:
    BuildTaskCurrent();
    ~BuildTaskCurrent();

    BuildTaskCurrent(const BuildTaskCurrent&) = delete;
    BuildTaskCurrent& operator=(const BuildTaskCurrent&) = delete;
    BuildTaskCurrent(BuildTaskCurrent&&) = delete;
    BuildTaskCurrent& operator=(BuildTaskCurrent&&) = delete;

public:
    void updateCurrent(const proto::VersionInfo& committedVersionInfo, indexlibv2::versionid_t alignFailedVersionId,
                       bool buildFinished, const std::shared_ptr<BuildTaskTarget>& target);
    bool reachedTarget(const std::shared_ptr<BuildTaskTarget>& target);
    indexlibv2::versionid_t getOpenedPublishedVersion() const { return _latestOpenedPublishedVersion; }
    bool isBuildFinished() const { return _buildFinished; }
    bool isManualMergeFinished() const { return _manualMergeFinished; }
    void updateOpenedPublishedVersion(indexlibv2::versionid_t versionId) { _latestOpenedPublishedVersion = versionId; }
    void markManualMergeFinished() { _manualMergeFinished = true; }
    indexlibv2::versionid_t getAlignVersionId() const
    {
        return _buildCurrentInfo.lastAlignedVersion.versionMeta.GetVersionId();
    }
    proto::VersionInfo getCommitedVersionInfo() const { return _buildCurrentInfo.commitedVersion; }
    std::string getTaskStatus() const;

private:
    mutable std::mutex _targetMutex;
    std::shared_ptr<BuildTaskTarget> _reachedTarget;
    bool _buildFinished = false;
    bool _manualMergeFinished = false;
    proto::BuildTaskCurrentInfo _buildCurrentInfo;
    indexlibv2::versionid_t _latestOpenedPublishedVersion = indexlibv2::INVALID_VERSIONID;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuildTaskCurrent);

}} // namespace build_service::build_task
