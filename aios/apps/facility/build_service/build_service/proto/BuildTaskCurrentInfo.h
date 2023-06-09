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

#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "indexlib/base/Constant.h"
#include "indexlib/framework/VersionMeta.h"

namespace build_service::proto {

struct VersionInfo : public autil::legacy::Jsonizable {
    std::string indexRoot;
    int64_t totalRemainIndexSize = 0;
    indexlibv2::framework::VersionMeta versionMeta;
    bool operator==(const VersionInfo& other) const
    {
        return totalRemainIndexSize == other.totalRemainIndexSize && indexRoot == other.indexRoot &&
               versionMeta == other.versionMeta;
    }

    bool operator!=(const VersionInfo& other) const { return !((*this) == other); }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("index_root", indexRoot, indexRoot);
        json.Jsonize("total_remain_index_size", totalRemainIndexSize, totalRemainIndexSize);
        json.Jsonize("version_meta", versionMeta, versionMeta);
    }
};

struct BuildTaskCurrentInfo : public autil::legacy::Jsonizable {
    BuildTaskCurrentInfo() = default;

    VersionInfo commitedVersion;
    VersionInfo lastAlignedVersion;
    indexlibv2::versionid_t lastAlignFailedVersionId = indexlibv2::INVALID_VERSIONID;
    bool operator==(const BuildTaskCurrentInfo& other) const
    {
        return commitedVersion == other.commitedVersion && lastAlignedVersion == other.lastAlignedVersion &&
               lastAlignFailedVersionId == other.lastAlignFailedVersionId;
    }

    bool operator!=(const BuildTaskCurrentInfo& other) const { return !((*this) == other); }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("commited_version", commitedVersion, commitedVersion);
        json.Jsonize("last_aligned_version", lastAlignedVersion, lastAlignedVersion);
        json.Jsonize("last_align_failed_version", lastAlignFailedVersionId, lastAlignFailedVersionId);
    }
};

} // namespace build_service::proto
