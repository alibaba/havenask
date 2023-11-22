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

#include <stdint.h>
#include <string>

#include "build_service/common_define.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/proto/BuildTaskTargetInfo.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/util/Log.h"
#include "indexlib/base/Types.h"

namespace indexlibv2 { namespace framework {
class VersionCoord;
}} // namespace indexlibv2::framework

namespace build_service { namespace build_task {

class BuildTaskTarget
{
public:
    BuildTaskTarget();
    ~BuildTaskTarget();

    BuildTaskTarget(const BuildTaskTarget&) = delete;
    BuildTaskTarget& operator=(const BuildTaskTarget&) = delete;
    BuildTaskTarget(BuildTaskTarget&&) = delete;
    BuildTaskTarget& operator=(BuildTaskTarget&&) = delete;

public:
    bool init(const config::TaskTarget& target);
    proto::BuildMode getBuildMode() const { return _targetInfo.buildMode; }
    const std::string& getBuildStep() const { return _targetInfo.buildStep; }
    const std::string& getIndexRoot() const { return _targetInfo.indexRoot; }
    uint64_t getBranchId() const { return _targetInfo.branchId; }
    bool isFinished() const { return _targetInfo.finished; }
    indexlibv2::versionid_t getLatestPublishedVersion() const;
    bool operator==(const BuildTaskTarget& other)
    {
        return _targetInfo == other._targetInfo && _dataDescription == other._dataDescription;
    }
    indexlibv2::versionid_t getAlignVersionId() const;
    indexlibv2::framework::VersionCoord getVersionCoord() const;
    bool needSkipMerge() const { return _targetInfo.skipMerge; }
    std::string getSpecifyMergeConfig() const { return _targetInfo.specificMergeConfig; }
    int32_t getBatchId() const { return _targetInfo.batchId; }
    bool isBatchBuild() const { return _targetInfo.batchId != -1; }
    bool getBatchMask(std::string* batchMask) const;
    bool getSrcSignature(uint64_t* srcSignature) const;
    const proto::DataDescription& getDataDescription() const { return _dataDescription; }
    const proto::BuildTaskTargetInfo& getBuildTaskTargetInfo() const { return _targetInfo; }

private:
    bool checkBuilderParam(const proto::BuildTaskTargetInfo& buildTaskInfo) const;
    proto::BuildTaskTargetInfo _targetInfo;
    proto::DataDescription _dataDescription;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuildTaskTarget);

}} // namespace build_service::build_task
