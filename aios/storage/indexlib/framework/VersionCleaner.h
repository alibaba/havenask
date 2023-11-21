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

#include <map>
#include <memory>
#include <set>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "fslib/common/common_type.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/Fence.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/VersionCoord.h"

namespace indexlibv2::framework {

class SegmentFenceDirFinder;

class VersionCleaner
{
public:
    VersionCleaner() = default;
    virtual ~VersionCleaner() = default;

    struct VersionCleanerOptions {
        int64_t fenceTsTolerantDeviation = 3600l * 1000 * 1000;
        uint32_t keepVersionCount = 1;
        uint32_t keepVersionHour = 0;
        versionid_t currentMaxVersionId = INVALID_VERSIONID;
        bool ignoreKeepVersionCount = false;
    };

    Status Clean(const std::shared_ptr<indexlib::file_system::IDirectory>& rootDir,
                 const VersionCleanerOptions& options, const std::set<VersionCoord>& reservedVersions = {});

protected:
    // virtual for test
    virtual std::pair<Status, std::shared_ptr<SegmentFenceDirFinder>>
    CreateSegmentFenceFinder(const std::string& root, const Version& version) const;

private:
    struct VersionInfo {
        Version version;
        std::shared_ptr<SegmentFenceDirFinder> fenceFinder;
        bool IsValid() const { return version.IsValid() && (fenceFinder != nullptr); }
    };

    Status CollectAllVersionsInRoot();
    Status CollectFilesInRootDir();
    Status CollectAllReservedSegments();
    Status CollectReservedPrivateVersions();

    // fill reserved versions
    void FillReservedVersionsByEnv();
    void FillReservedVersionsByKeepCount();
    void FillReservedVersionsByKeepHour();
    void FillReservedVersionsByMergedVersion();
    void AddReservedVersion(versionid_t versionId);

    Status CalculateFenceTsThresholds();

    // fill to remove
    void FillSegmentsToRemove();
    void FillVersionsToRemove();
    Status FillFencesToRemove();

    Status DoCleanFiles();

    VersionInfo GetVersionInfo(versionid_t versionId) const;

    void Reset();

    Status CleanPatchIndex();
    Status CleanVersionFile(const std::string& fenceName, versionid_t versionId);
    static void GetMergedSegmentId(const Version& version, segmentid_t* minSegId, segmentid_t* maxSegId);
    size_t GetPublicVersionCount(const std::vector<VersionInfo>& versionInfos);
    Status CollectSegmentInDir(const std::string& dirName, std::map<std::string, fslib::FileList>* segList);

    Status CreateVersionInfo(const std::shared_ptr<indexlib::file_system::IDirectory>& dir, versionid_t versionId,
                             VersionInfo* info) const;

    template <typename T>
    std::vector<std::string> VersionCoordsToStrs(const T& versionCoords) const
    {
        std::vector<std::string> res;
        for (const auto& versionCoord : versionCoords) {
            res.push_back(versionCoord.DebugString());
        }
        return res;
    }

private:
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;
    VersionCleanerOptions _options;
    std::set<VersionCoord> _reservedVersions;
    // segFileName
    std::set<std::string> _reservedSegments;
    std::set<std::string> _reservedFences;

    std::vector<VersionInfo> _allVersionsInRoot;
    std::vector<VersionInfo> _versionsJustCommitted;
    std::vector<VersionInfo> _reservedPrivateVersions;

    std::vector<std::string> _allFences;
    std::vector<std::string> _allPatchNames;
    // segFileName : fenceName
    std::set<std::pair<std::string, std::string>> _allSegments;

    // segFileName : fenceName
    std::vector<std::pair<std::string, std::string>> _segmentsToRemove;
    std::vector<std::string> _fencesToRemove;
    std::vector<VersionCoord> _versionsToRemove;

    std::map<std::pair<uint16_t, uint16_t>, Fence::FenceMeta> _rangeToFenceMeta;

    std::map<std::string, fslib::FileList> _patchSegList;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
