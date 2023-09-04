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
#include "indexlib/framework/VersionCleaner.h"

#include <memory>

#include "autil/EnvUtil.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentFenceDirFinder.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/VersionCoord.h"
#include "indexlib/framework/VersionLoader.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, VersionCleaner);

Status VersionCleaner::Clean(const std::shared_ptr<indexlib::file_system::IDirectory>& rootDir,
                             const VersionCleanerOptions& options, const std::set<VersionCoord>& reservedVersions)
{
    Reset();
    if (rootDir == nullptr) {
        auto status = Status::InvalidArgs("Init failed: root dir is nullptr");
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    if (options.currentMaxVersionId == INVALID_VERSIONID) {
        auto status = Status::InvalidArgs("Current max version id is INVALID_VERSIONID, "
                                          "all versions will not be cleaned.");
        AUTIL_LOG(WARN, "%s", status.ToString().c_str());
    }
    _rootDir = rootDir;
    _options = options;

    AUTIL_LOG(INFO, "Version cleaner init with reserved versions %s",
              autil::legacy::ToJsonString(VersionCoordsToStrs(reservedVersions), /*isCompact=*/true).c_str());

    if (_options.keepVersionCount == 0) {
        AUTIL_LOG(WARN,
                  "Keep version count [%u] should not be less than 1. "
                  "use 1 by default.",
                  _options.keepVersionCount);
        _options.keepVersionCount = 1;
    }
    if (_options.fenceTsTolerantDeviation < 0) {
        AUTIL_LOG(WARN,
                  "Fence timestamp deviation [%ld] should not be less than 0. "
                  "use 0 by default.",
                  _options.fenceTsTolerantDeviation);
        _options.fenceTsTolerantDeviation = 0;
    }
    if (_options.keepVersionCount == INVALID_KEEP_VERSION_COUNT) {
        AUTIL_LOG(WARN, "Not trigger clean, keepVersionCount[%u] is INVALID_KEEP_VERSION_COUNT",
                  _options.keepVersionCount);
        return Status::OK();
    }

    AUTIL_LOG(INFO,
              "Begin version clean, keepVersionCount[%u], keepVersionHour[%u], currentMaxVersionId[%d], "
              "fenceTsTolerantDeviation[%ld]",
              _options.keepVersionCount, _options.keepVersionHour, _options.currentMaxVersionId,
              _options.fenceTsTolerantDeviation);

    // fill all versions, all fences and reserved versions
    auto status = CollectAllVersionsInRoot();
    RETURN_IF_STATUS_ERROR(status, "Collect all versions in rootDir[%s] failed.", rootDir->GetOutputPath().c_str());
    size_t publicVersionCount = GetPublicVersionCount(_allVersionsInRoot);
    if (publicVersionCount <= _options.keepVersionCount) {
        AUTIL_LOG(INFO,
                  "PublicVersionCount [%lu] is less equal than keepVersionCount[%u], "
                  "no need to clean.",
                  publicVersionCount, _options.keepVersionCount);
        return Status::OK();
    }

    status = CollectFilesInRootDir();
    RETURN_IF_STATUS_ERROR(status, "Collect files in rootDir[%s] failed.", rootDir->GetOutputPath().c_str());

    for (const auto& reservedVersion : reservedVersions) {
        if (reservedVersion.GetVersionFenceName() == "") {
            AddReservedVersion(reservedVersion.GetVersionId());
        } else {
            _reservedVersions.emplace(reservedVersion);
        }
    }

    FillReservedVersionsByEnv();
    FillReservedVersionsByKeepCount();
    FillReservedVersionsByKeepHour();
    FillReservedVersionsByMergedVersion();

    status = CollectReservedPrivateVersions();
    RETURN_IF_STATUS_ERROR(status, "Collect reserved private versions failed, rootDir[%s]",
                           rootDir->GetOutputPath().c_str());

    // fill all segments, reserved segments and reserved fences
    std::vector<VersionInfo> allVersions = _allVersionsInRoot;
    allVersions.insert(allVersions.end(), _reservedPrivateVersions.begin(), _reservedPrivateVersions.end());
    allVersions.insert(allVersions.end(), _versionsJustCommitted.begin(), _versionsJustCommitted.end());
    for (const auto& versionInfo : allVersions) {
        const auto& version = versionInfo.version;
        bool isReservedVersion =
            (_reservedVersions.count(VersionCoord(version.GetVersionId(), version.GetFenceName())) != 0);
        if (isReservedVersion) {
            _reservedFences.insert(version.GetFenceName());
        } else {
            isReservedVersion = (_reservedVersions.count(VersionCoord(version.GetVersionId(), "")) != 0);
        }
        for (size_t i = 0; i < versionInfo.version.GetSegmentCount(); i++) {
            segmentid_t segId = version[i];
            auto [status, fenceName] = versionInfo.fenceFinder->GetFenceDir(segId);
            RETURN_IF_STATUS_ERROR(status, "Find fence dir from segment [%d] failed", segId);
            std::string segFileName = version.GetSegmentDirName(segId);
            if (isReservedVersion) {
                _reservedSegments.insert(segFileName);
                _reservedFences.insert(fenceName);
            }
            _allSegments.insert({segFileName, fenceName});
        }
    }
    _reservedFences.insert("");

    AUTIL_LOG(INFO, "Reserved versions %s",
              autil::legacy::ToJsonString(VersionCoordsToStrs(_reservedVersions), /*isCompact=*/true).c_str());
    AUTIL_LOG(INFO, "Reserved segments %s", autil::legacy::ToJsonString(_reservedSegments, /*isCompact=*/true).c_str());
    AUTIL_LOG(INFO, "Reserved fences %s", autil::legacy::ToJsonString(_reservedFences, /*isCompact=*/true).c_str());

    FillSegmentsToRemove();
    FillVersionsToRemove();
    status = CalculateFenceTsThresholds();
    RETURN_IF_STATUS_ERROR(status, "Calculate fence ts thresholds failed, rootDir[%s]",
                           rootDir->GetOutputPath().c_str());
    status = FillFencesToRemove();
    RETURN_IF_STATUS_ERROR(status, "Fill fences to remove failed, rootDir[%s]", rootDir->GetOutputPath().c_str());

    status = DoCleanFiles();
    RETURN_IF_STATUS_ERROR(status, "Clean files failed, rootDir[%s]", rootDir->GetOutputPath().c_str());

    AUTIL_LOG(INFO, "Version clean finished");
    return Status::OK();
}

Status VersionCleaner::CreateVersionInfo(const std::shared_ptr<indexlib::file_system::IDirectory>& dir,
                                         versionid_t versionId, VersionInfo* info) const
{
    auto legacyDir = std::make_shared<indexlib::file_system::Directory>(dir);
    auto status1 = VersionLoader::GetVersion(legacyDir, versionId, &(info->version));
    RETURN_IF_STATUS_ERROR(status1, "Get version[%d] for dir[%s] failed", versionId, dir->GetOutputPath().c_str());
    auto fenceName = info->version.GetFenceName();
    auto entryTableName = PathUtil::JoinPath(fenceName, PathUtil::GetEntryTableFileName(info->version.GetVersionId()));
    auto [status2, isExist] = _rootDir->IsExist(entryTableName).StatusWith();
    RETURN_IF_STATUS_ERROR(status2, "Check entry table exist failed");
    if (!isExist) {
        // compitable with v1 legacy fileSystem
        auto deployMetaFileName =
            PathUtil::JoinPath(fenceName, PathUtil::GetDeployMetaFileName(info->version.GetVersionId()));
        auto [status, isExist] = _rootDir->IsExist(deployMetaFileName).StatusWith();
        RETURN_IF_STATUS_ERROR(status, "Check deploy meta exist failed");
        if (!isExist) {
            return Status::OK();
        }
    }
    std::string rootPhysicalPath = _rootDir->GetPhysicalPath("");
    auto [status3, fenceFinder] = CreateSegmentFenceFinder(rootPhysicalPath, info->version);
    RETURN_IF_STATUS_ERROR(status3, "Create segment fence finder failed, versionId[%d], path[%s]", versionId,
                           rootPhysicalPath.c_str());
    info->fenceFinder = fenceFinder;
    return Status::OK();
}

Status VersionCleaner::CollectReservedPrivateVersions()
{
    for (const auto& version : _reservedVersions) {
        const auto& versionId = version.GetVersionId();
        if (versionId & Version::PRIVATE_VERSION_ID_MASK) {
            auto [status, fenceDir] = _rootDir->GetDirectory(version.GetVersionFenceName()).StatusWith();
            RETURN_IF_STATUS_ERROR(status, "Get fence[%s] dir failed, versionId[%d]",
                                   version.GetVersionFenceName().c_str(), versionId);
            VersionInfo info;
            RETURN_IF_STATUS_ERROR(CreateVersionInfo(fenceDir, versionId, &info),
                                   "Create version info failed, fence[%s], versionId[%d].",
                                   version.GetVersionFenceName().c_str(), versionId);
            if (!info.IsValid()) {
                continue;
            }
            _reservedPrivateVersions.push_back(info);
            AUTIL_LOG(INFO, "Collected private reserved version [%d] in fence[%s]", info.version.GetVersionId(),
                      info.version.GetFenceName().c_str());
        }
    }
    return Status::OK();
}

Status VersionCleaner::CollectAllVersionsInRoot()
{
    auto dir = std::make_shared<indexlib::file_system::Directory>(_rootDir);
    fslib::FileList fileList;
    // listed versions are in version id ascending order.
    auto status = VersionLoader::ListVersion(dir, &fileList);
    RETURN_IF_STATUS_ERROR(status, "List version failed.");
    for (size_t i = 0; i < fileList.size(); i++) {
        const auto& fileName = fileList[i];
        versionid_t versionId = VersionLoader::GetVersionId(fileName);
        VersionInfo info;
        RETURN_IF_STATUS_ERROR(CreateVersionInfo(_rootDir, versionId, &info),
                               "Create version info failed, versionId[%d].", versionId);
        if (!info.IsValid()) {
            continue;
        }
        if (versionId > _options.currentMaxVersionId) {
            // maybe a new version has just been committed.
            _versionsJustCommitted.push_back(info);
            _reservedVersions.emplace(info.version.GetVersionId(), info.version.GetFenceName());
            AUTIL_LOG(INFO, "Collected new version [%d] fence[%s]", info.version.GetVersionId(),
                      info.version.GetFenceName().c_str());
        } else {
            _allVersionsInRoot.push_back(info);
            AUTIL_LOG(INFO, "Collected version [%d] fence[%s]", info.version.GetVersionId(),
                      info.version.GetFenceName().c_str());
        }
    }
    return Status::OK();
}

Status VersionCleaner::CollectFilesInRootDir()
{
    std::vector<std::string> fileList;
    auto status = _rootDir->ListDir("", indexlib::file_system::ListOption(), fileList).Status();
    RETURN_IF_STATUS_ERROR(status, "List all fence and patch dirs failed.");
    for (const auto& fileName : fileList) {
        if (Fence::IsFenceName(fileName)) {
            _allFences.push_back(fileName);
        } else if (PathUtil::IsPatchIndexDirName(fileName)) {
            _allPatchNames.push_back(fileName);
        }
        // do not remove not-in-version segment, to avoid removing new segment wrongly
    }
    return Status::OK();
}

void VersionCleaner::FillReservedVersionsByEnv()
{
    std::vector<versionid_t> reservedVersionsFromEnv;
    std::string versions;
    if (!autil::EnvUtil::getEnvWithoutDefault("KEEP_VERSIONS", versions)) {
        return;
    }
    autil::StringUtil::fromString(versions, reservedVersionsFromEnv, ";");
    for (auto versionId : reservedVersionsFromEnv) {
        if (versionId != INVALID_VERSIONID) {
            AddReservedVersion(versionId);
        }
    }
}

void VersionCleaner::FillReservedVersionsByKeepCount()
{
    size_t i = (_allVersionsInRoot.size() > _options.keepVersionCount)
                   ? (_allVersionsInRoot.size() - _options.keepVersionCount)
                   : 0;
    for (; i < _allVersionsInRoot.size(); i++) {
        const auto& version = _allVersionsInRoot[i].version;
        if (version.GetVersionId() != INVALID_VERSIONID) {
            AddReservedVersion(version.GetVersionId());
        }
    }
}

void VersionCleaner::FillReservedVersionsByKeepHour()
{
    int64_t timestampToReclaim = autil::TimeUtility::currentTimeInSeconds() - _options.keepVersionHour * 3600;
    AUTIL_LOG(INFO, "Reserve versions committed after ts[%ld].", timestampToReclaim);
    bool isLastVersionValid = false;
    for (auto iter = _allVersionsInRoot.rbegin(); iter != _allVersionsInRoot.rend(); ++iter) {
        const auto& versionInfo = *iter;
        int64_t commitTime = versionInfo.version.GetCommitTime();
        commitTime = autil::TimeUtility::us2sec(commitTime);
        const auto& version = versionInfo.version;
        if (commitTime > timestampToReclaim) {
            AddReservedVersion(version.GetVersionId());
            isLastVersionValid = true;
        } else {
            if (isLastVersionValid) {
                AddReservedVersion(version.GetVersionId());
            }
            isLastVersionValid = false;
        }
    }
}

void VersionCleaner::FillReservedVersionsByMergedVersion()
{
    if (_allVersionsInRoot.size() <= _options.keepVersionCount) {
        return;
    }
    const Version& minVersion = _allVersionsInRoot[_allVersionsInRoot.size() - _options.keepVersionCount].version;
    int64_t minVersionCommitTime = minVersion.GetCommitTime();

    segmentid_t minMergedSegmentId;
    segmentid_t maxMergedSegmentId;
    GetMergedSegmentId(minVersion, &minMergedSegmentId, &maxMergedSegmentId);
    bool found = false;
    for (size_t i = 0; i < _allVersionsInRoot.size(); i++) {
        versionid_t versionId = _allVersionsInRoot[i].version.GetVersionId();
        if (versionId & Version::PUBLIC_VERSION_ID_MASK) {
            break;
        }
        if (_allVersionsInRoot[i].version.GetCommitTime() >= minVersionCommitTime) {
            AddReservedVersion(versionId);
        }
        if (found) {
            AddReservedVersion(versionId);
            continue;
        }
        segmentid_t tmpMinSegId;
        segmentid_t tmpMaxSegId;
        GetMergedSegmentId(_allVersionsInRoot[i].version, &tmpMinSegId, &tmpMaxSegId);
        if ((tmpMinSegId == minMergedSegmentId && tmpMaxSegId == maxMergedSegmentId) ||
            minMergedSegmentId == INVALID_SEGMENTID) {
            found = true;
            AddReservedVersion(versionId);
        }
    }
}

void VersionCleaner::FillSegmentsToRemove()
{
    for (const auto& [segFileName, fenceName] : _allSegments) {
        if (_reservedSegments.count(segFileName) == 0) {
            _segmentsToRemove.emplace_back(segFileName, fenceName);
        }
    }
    AUTIL_LOG(INFO, "Segments to remove: %s.",
              autil::legacy::ToJsonString(_segmentsToRemove, /*isCompact=*/true).c_str());
}

void VersionCleaner::FillVersionsToRemove()
{
    for (const auto& versionInfo : _allVersionsInRoot) {
        const auto& version = versionInfo.version;
        const VersionCoord versionCoord(version.GetVersionId(), version.GetFenceName());
        if (_reservedVersions.count(versionCoord) == 0) {
            _versionsToRemove.push_back(versionCoord);
        }
    }
    AUTIL_LOG(INFO, "Versions to remove: %s.",
              autil::legacy::ToJsonString(VersionCoordsToStrs(_versionsToRemove), /*isCompact=*/true).c_str());
}

Status VersionCleaner::FillFencesToRemove()
{
    for (const std::string& fenceName : _allFences) {
        if (_reservedFences.count(fenceName) == 0) {
            auto [res, fenceMeta] = Fence::DecodePublicFence(fenceName);
            if (!res) {
                AUTIL_LOG(ERROR, "decode fence [%s] failed", fenceName.c_str());
                return Status::InternalError();
            }
            // TODO(yonghao.fyh) : consider clean up after full build fence
            auto iter = _rangeToFenceMeta.find(fenceMeta.range);
            if (iter == _rangeToFenceMeta.end()) {
                continue;
            }
            const auto& latestFenceMeta = iter->second;
            if (fenceMeta.timestamp < latestFenceMeta.timestamp - _options.fenceTsTolerantDeviation) {
                _fencesToRemove.push_back(fenceName);
                AUTIL_LOG(INFO,
                          "Fill remove fence[%s], timestamp[%ld]. latest fence[%s], timestamp[%ld]. range[%u_%u].",
                          fenceName.c_str(), fenceMeta.timestamp, latestFenceMeta.fenceName.c_str(),
                          latestFenceMeta.timestamp, fenceMeta.range.first, fenceMeta.range.second);
            }
        }
    }
    AUTIL_LOG(INFO, "Fences to remove: %s", autil::legacy::ToJsonString(_fencesToRemove, /*isCompact=*/true).c_str());
    return Status::OK();
}

Status VersionCleaner::CalculateFenceTsThresholds()
{
    for (const auto& versionCoord : _reservedVersions) {
        if (versionCoord.GetVersionFenceName().empty()) {
            continue;
        }
        auto [res, fenceMeta] = Fence::DecodePublicFence(versionCoord.GetVersionFenceName());
        if (!res) {
            return Status::InternalError("Decode public fence name [%s] failed.",
                                         versionCoord.GetVersionFenceName().c_str());
        }
        if (_rangeToFenceMeta.find(fenceMeta.range) == _rangeToFenceMeta.end() ||
            fenceMeta.timestamp > _rangeToFenceMeta[fenceMeta.range].timestamp) {
            _rangeToFenceMeta[fenceMeta.range] = fenceMeta;
        }
    }
    if (!_rangeToFenceMeta.empty()) {
        AUTIL_LOG(INFO, "Range to fence meta %s.",
                  autil::legacy::ToJsonString(_rangeToFenceMeta, /*isCompact=*/true).c_str());
    }
    return Status::OK();
}

Status VersionCleaner::DoCleanFiles()
{
    auto status = CleanPatchIndex();
    RETURN_IF_STATUS_ERROR(status, "Clean patch index failed.");
    for (const auto& versionCoord : _versionsToRemove) {
        std::string versionFenceName = versionCoord.GetVersionFenceName();
        if (versionFenceName != "") {
            auto status = CleanVersionFile(versionFenceName, versionCoord.GetVersionId());
            RETURN_IF_STATUS_ERROR(status, "Clean version [%d] failed", versionCoord.GetVersionId());
        }
        auto status = CleanVersionFile("", versionCoord.GetVersionId());
        RETURN_IF_STATUS_ERROR(status, "clean version [%d] failed", versionCoord.GetVersionId());
        AUTIL_LOG(INFO, "Version [%s] is removed", versionCoord.DebugString().c_str());
    }
    for (const auto& [segFileName, fenceName] : _segmentsToRemove) {
        auto segDirName = PathUtil::JoinPath(fenceName, segFileName);
        auto status =
            _rootDir->RemoveDirectory(segDirName, indexlib::file_system::RemoveOption::MayNonExist()).Status();
        RETURN_IF_STATUS_ERROR(status, "remove seg dir [%s] failed", segDirName.c_str());
        AUTIL_LOG(INFO, "Segment [%s] is removed", segDirName.c_str());
    }
    for (const auto& fence : _fencesToRemove) {
        auto status = _rootDir->RemoveDirectory(fence, indexlib::file_system::RemoveOption::MayNonExist()).Status();
        RETURN_IF_STATUS_ERROR(status, "remove fence dir [%s] failed", fence.c_str());
        AUTIL_LOG(INFO, "Fence [%s] is removed", fence.c_str());
    }
    return Status::OK();
}

void VersionCleaner::GetMergedSegmentId(const Version& version, segmentid_t* minSegId, segmentid_t* maxSegId)
{
    (*minSegId) = INVALID_SEGMENTID;
    (*maxSegId) = INVALID_SEGMENTID;
    for (size_t i = 0; i < version.GetSegmentCount(); i++) {
        const auto& segId = version[i];
        if (Segment::IsMergedSegmentId(segId)) {
            if ((*minSegId) == INVALID_SEGMENTID) {
                (*minSegId) = segId;
                (*maxSegId) = segId;
            } else {
                (*minSegId) = std::min((*minSegId), segId);
                (*maxSegId) = std::max((*maxSegId), segId);
            }
        }
    }
}

std::pair<Status, std::shared_ptr<SegmentFenceDirFinder>>
VersionCleaner::CreateSegmentFenceFinder(const std::string& root, const Version& version) const
{
    auto fenceFinder = std::make_shared<SegmentFenceDirFinder>();
    RETURN2_IF_STATUS_ERROR(fenceFinder->Init(root, version), nullptr, "init fence finder for version [%d] failed",
                            version.GetVersionId());
    return {Status::OK(), fenceFinder};
}

VersionCleaner::VersionInfo VersionCleaner::GetVersionInfo(versionid_t versionId) const
{
    for (const auto& versionInfo : _allVersionsInRoot) {
        if (versionInfo.version.GetVersionId() == versionId) {
            return versionInfo;
        }
    }
    for (const auto& versionInfo : _reservedPrivateVersions) {
        if (versionInfo.version.GetVersionId() == versionId) {
            return versionInfo;
        }
    }
    for (const auto& versionInfo : _versionsJustCommitted) {
        if (versionInfo.version.GetVersionId() == versionId) {
            return versionInfo;
        }
    }
    return VersionInfo();
}

void VersionCleaner::AddReservedVersion(versionid_t versionId)
{
    auto versionInfo = GetVersionInfo(versionId);
    const auto& version = versionInfo.version;
    if (version.GetVersionId() != INVALID_VERSIONID) {
        _reservedVersions.emplace(version.GetVersionId(), version.GetFenceName());
    } else {
        _reservedVersions.emplace(versionId);
    }
}

Status VersionCleaner::CleanVersionFile(const std::string& fenceName, versionid_t versionId)
{
    auto fenceVersionName = PathUtil::JoinPath(fenceName, Version::GetVersionFileName(versionId));
    auto status = _rootDir->RemoveFile(fenceVersionName, indexlib::file_system::RemoveOption::MayNonExist()).Status();
    RETURN_IF_STATUS_ERROR(status, "Remove version file [%s] failed", fenceVersionName.c_str());
    auto entryTableName = PathUtil::JoinPath(fenceName, PathUtil::GetEntryTableFileName(versionId));
    status = _rootDir->RemoveFile(entryTableName, indexlib::file_system::RemoveOption::MayNonExist()).Status();
    RETURN_IF_STATUS_ERROR(status, "Remove entry table file [%s] failed", entryTableName.c_str());
    auto deployMetaName = PathUtil::JoinPath(fenceName, PathUtil::GetDeployMetaFileName(versionId));
    status = _rootDir->RemoveFile(deployMetaName, indexlib::file_system::RemoveOption::MayNonExist()).Status();
    RETURN_IF_STATUS_ERROR(status, "Remove deploy meta file [%s] failed", entryTableName.c_str());

    return Status::OK();
}

size_t VersionCleaner::GetPublicVersionCount(const std::vector<VersionInfo>& versionInfos)
{
    size_t publicVersionCount = 0;
    for (size_t i = 0; i < versionInfos.size(); i++) {
        versionid_t versionId = versionInfos[i].version.GetVersionId();
        if (versionId & Version::PUBLIC_VERSION_ID_MASK) {
            publicVersionCount++;
        }
    }
    return publicVersionCount;
}

Status VersionCleaner::CollectSegmentInDir(const std::string& dirName, std::map<std::string, fslib::FileList>* segList)
{
    fslib::FileList fileList;
    auto [status, isExist] = _rootDir->IsExist(dirName).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "Check fence dir exist failed");
    if (!isExist) {
        return Status::OK();
    }
    auto [status1, fenceDir] = _rootDir->GetDirectory(dirName).StatusWith();
    RETURN_IF_STATUS_ERROR(status1, "Get sub fence dir failed");
    auto dir = std::make_shared<indexlib::file_system::Directory>(fenceDir);
    status = VersionLoader::ListSegment(dir, &fileList);
    RETURN_IF_STATUS_ERROR(status, "List segment failed");
    if (fileList.empty()) {
        return Status::OK();
    }
    (*segList)[dirName] = fileList;
    return Status::OK();
}

Status VersionCleaner::CleanPatchIndex()
{
    for (const auto& patchName : _allPatchNames) {
        auto status = CollectSegmentInDir(patchName, &_patchSegList);
        RETURN_IF_STATUS_ERROR(status, "Collect segments in patch index dir [%s] failed", patchName.c_str());
    }
    // clean patch segments
    for (const auto& [patchDir, segFileNames] : _patchSegList) {
        bool allCleaned = true;
        for (const auto& segFileName : segFileNames) {
            bool isToRemove = false;
            for (const auto& [segName, _] : _segmentsToRemove) {
                if (segName == segFileName) {
                    isToRemove = true;
                    break;
                }
            }
            if (isToRemove) {
                auto segDirName = PathUtil::JoinPath(patchDir, segFileName);
                auto status =
                    _rootDir->RemoveDirectory(segDirName, indexlib::file_system::RemoveOption::MayNonExist()).Status();
                RETURN_IF_STATUS_ERROR(status, "Remove patch seg dir [%s] failed", segDirName.c_str());
                AUTIL_LOG(INFO, "Patch segment: [%s] removed", segDirName.c_str());
            } else {
                allCleaned = false;
            }
        }
        if (allCleaned && !patchDir.empty()) {
            auto status =
                _rootDir->RemoveDirectory(patchDir, indexlib::file_system::RemoveOption::MayNonExist()).Status();
            RETURN_IF_STATUS_ERROR(status, "remove patch dir [%s] failed", patchDir.c_str());
            AUTIL_LOG(INFO, "Patch dir: [%s] removed", patchDir.c_str());
        }
    }
    return Status::OK();
}

void VersionCleaner::Reset()
{
    _rootDir.reset();
    _options = VersionCleanerOptions();
    _reservedVersions.clear();
    _reservedSegments.clear();
    _reservedFences.clear();
    _allVersionsInRoot.clear();
    _allFences.clear();
    _allPatchNames.clear();
    _allSegments.clear();
    _segmentsToRemove.clear();
    _fencesToRemove.clear();
    _versionsToRemove.clear();
    _rangeToFenceMeta.clear();
    _patchSegList.clear();
    _versionsJustCommitted.clear();
    _reservedPrivateVersions.clear();
}

} // namespace indexlibv2::framework
