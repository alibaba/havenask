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

#include <string>
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "indexlib/base/Constant.h"

namespace build_service::proto {

struct VersionProgress : public autil::legacy::Jsonizable {
    VersionProgress() {}
    VersionProgress(const std::string& fence_, indexlibv2::versionid_t versionId_)
        : versionId(versionId_)
        , fence(fence_)
    {
    }

    indexlibv2::versionid_t versionId = indexlibv2::INVALID_VERSIONID;
    std::string fence = "";
    bool operator==(const VersionProgress& other) const { return versionId == other.versionId && fence == other.fence; }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("version_id", versionId, versionId);
        json.Jsonize("fence", fence, fence);
    }
};

enum BuildMode { UNKOWN = 0x0, BUILD = 0x01, PUBLISH = 0x02, BUILD_PUBLISH = BUILD | PUBLISH };

struct BuildTaskTargetInfo : public autil::legacy::Jsonizable {
    bool finished = false;
    bool skipMerge = false;
    int32_t batchId = -1;
    proto::BuildMode buildMode = UNKOWN;
    indexlibv2::versionid_t alignVersionId = indexlibv2::INVALID_VERSIONID;
    indexlibv2::versionid_t latestPublishedVersion = indexlibv2::INVALID_VERSIONID;
    uint64_t branchId = 0;
    std::string indexRoot = "";
    std::string buildStep = "";
    std::vector<VersionProgress> slaveVersionProgress;
    bool operator==(const BuildTaskTargetInfo& other) const
    {
        return finished == other.finished && buildMode == other.buildMode && alignVersionId == other.alignVersionId &&
               latestPublishedVersion == other.latestPublishedVersion && branchId == other.branchId &&
               indexRoot == other.indexRoot && buildStep == other.buildStep &&
               slaveVersionProgress == other.slaveVersionProgress &&
               skipMerge == other.skipMerge && batchId == other.batchId;
    }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("finished", finished, finished);
        json.Jsonize("build_mode", buildMode, buildMode);
        json.Jsonize("index_root", indexRoot, indexRoot);
        json.Jsonize("align_version_id", alignVersionId, alignVersionId);
        json.Jsonize("buildStep", buildStep, buildStep);
        json.Jsonize("branch_id", branchId, branchId);
        json.Jsonize("latest_published_version", latestPublishedVersion, latestPublishedVersion);
        json.Jsonize("slave_version_progress", slaveVersionProgress, slaveVersionProgress);
        json.Jsonize("skip_merge", skipMerge, skipMerge);
        json.Jsonize("batch_id", batchId, batchId);
    }
};
} // namespace build_service::proto
