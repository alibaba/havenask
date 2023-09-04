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
#include "indexlib/table/common/CommonVersionImporter.h"

#include "autil/Scope.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/framework/Fence.h"
#include "indexlib/framework/SegmentDescriptions.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/util/PathUtil.h"
using namespace indexlib::util;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib., CommonVersionImporter);

std::string CommonVersionImporter::NOT_SUPPORT = "not_support";
std::string CommonVersionImporter::KEEP_SEGMENT_IGNORE_LOCATOR = "keep_segment_ignore_locator";
std::string CommonVersionImporter::KEEP_SEGMENT_OVERWRITE_LOCATOR = "keep_segment_overwrite_locator";
std::string CommonVersionImporter::COMMON_TYPE = "common_type";

Status CommonVersionImporter::Check(const std::vector<framework::Version>& versions,
                                    const framework::Version* baseVersion, const framework::ImportOptions& options,
                                    std::vector<framework::Version>* validVersions)
{
    if (baseVersion == nullptr) {
        auto status = Status::InternalError("base version is nullptr");
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    if (versions.empty()) {
        return Status::Exist();
    }
    auto importLocator = versions[0].GetLocator();
    for (size_t i = 1; i < versions.size(); i++) {
        if (!importLocator.IsSameSrc(versions[i].GetLocator(), false)) {
            AUTIL_LOG(ERROR, "import version src not same");
            return Status::InvalidArgs();
        }
    }

    for (const auto& version : versions) {
        bool allSegmentImported = true;
        for (auto segInVersion : version) {
            if (!baseVersion->HasSegment(segInVersion.segmentId)) {
                allSegmentImported = false;
                break;
            }
        }
        if (allSegmentImported) {
            continue;
        }
        const auto& locator = version.GetLocator();
        if (!locator.GetUserData().empty()) {
            AUTIL_LOG(WARN, "locator user data not empty [%s], will not be merged", locator.GetUserData().c_str());
        }
        if (!locator.IsSameSrc(baseVersion->GetLocator(), false)) {
            auto importStrategy = options.GetImportStrategy();
            if (importStrategy == NOT_SUPPORT) {
                AUTIL_LOG(ERROR, "version src not same, import failed");
                return Status::InvalidArgs();
            }
            assert(importStrategy == KEEP_SEGMENT_IGNORE_LOCATOR || importStrategy == KEEP_SEGMENT_OVERWRITE_LOCATOR);
            (*validVersions).emplace_back(version);
            continue;
        }

        if (baseVersion->GetLocator().IsFasterThan(locator, false) !=
            framework::Locator::LocatorCompareResult::LCR_FULLY_FASTER) {
            (*validVersions).emplace_back(version);
        }
    }

    if ((*validVersions).empty()) {
        return Status::Exist();
    }
    return Status::OK();
}

std::pair<Status, framework::Locator>
CommonVersionImporter::GetSegmentLocator(const std::shared_ptr<indexlib::file_system::IDirectory>& segDir)
{
    framework::Locator segLocator;
    if (!segDir) {
        AUTIL_LOG(ERROR, "segment dir is nullptr");
        return std::make_pair(Status::InvalidArgs(), segLocator);
    }
    // TODO(yonghao.fyh) : readerOption check
    auto readerOption = indexlib::file_system::ReaderOption::PutIntoCache(indexlib::file_system::FSOT_MEM);
    framework::SegmentInfo segmentInfo;
    if (!segmentInfo.Load(segDir, readerOption).IsOK()) {
        AUTIL_LOG(ERROR, "load segment info[%s] failed", segDir->GetLogicalPath().c_str());
        return std::make_pair(Status::Corruption("load segment info failed"), segLocator);
    }
    return {Status::OK(), segmentInfo.GetLocator()};
}

Status CommonVersionImporter::Import(const std::vector<framework::Version>& versions, const framework::Fence* fence,
                                     const framework::ImportOptions& options, framework::Version* baseVersion)
{
    if (fence == nullptr) {
        auto status = Status::InvalidArgs("Fence Description is nullptr!");
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }

    // 1 check input versions
    std::vector<framework::Version> validVersions;
    auto status = Check(versions, baseVersion, options, &validVersions);
    RETURN_IF_STATUS_ERROR(status, "version import check failed");

    // 2 calculate new locator
    framework::Locator resultLocator = baseVersion->GetLocator();
    uint32_t formatVersion = 0;
    int64_t resultMaxTs = 0;
    for (const auto& version : validVersions) {
        auto versionLocator = version.GetLocator();
        if (!resultLocator.IsSameSrc(versionLocator, false)) {
            if (options.GetImportStrategy() == KEEP_SEGMENT_OVERWRITE_LOCATOR) {
                resultLocator = versionLocator;
            }
        } else {
            if (!resultLocator.Update(version.GetLocator())) {
                AUTIL_LOG(ERROR,
                          "locator update failed when import version[%d] with locator[%s]."
                          "  base locator is [%s]. current result locator is [%s]",
                          version.GetVersionId(), version.GetLocator().DebugString().c_str(),
                          baseVersion->GetLocator().DebugString().c_str(), resultLocator.DebugString().c_str());
                return Status::InternalError();
            }
        }
        if (version.GetFormatVersion() > formatVersion) {
            formatVersion = version.GetFormatVersion();
            resultMaxTs = version.GetTimestamp();
        } else {
            resultMaxTs = std::max(resultMaxTs, version.GetTimestamp());
        }
    }

    // 3 import new segments
    std::vector<std::shared_ptr<framework::SegmentDescriptions>> otherSegDescs;
    std::set<segmentid_t> newSegments;
    // rm version file in logical fs at the end to avoid conflict
    std::set<versionid_t> mountedVersionIds;
    bool removeVersionRet = true;
    {
        autil::ScopeGuard sg([&mountedVersionIds, &fence, &removeVersionRet]() {
            for (const auto& versionId : mountedVersionIds) {
                std::string versionFileName = VERSION_FILE_NAME_PREFIX + std::string(".") + std::to_string(versionId);
                indexlib::file_system::RemoveOption removeOption;
                removeOption.logicalDelete = true;
                removeOption.mayNonExist = true;
                auto status = fence->GetFileSystem()->RemoveFile(versionFileName, removeOption).Status();
                if (!status.IsOK()) {
                    removeVersionRet = false;
                    AUTIL_LOG(ERROR, "logical remove file %s failed", versionFileName.c_str());
                } else {
                    AUTIL_LOG(INFO, "logical remove file %s", versionFileName.c_str());
                }
            }
        });
        auto root = indexlib::file_system::Directory::Get(fence->GetFileSystem());

        // check if each input segment's locator is ahead of baseVersion's locator
        for (const auto& validVersion : validVersions) {
            const std::string fenceRoot = PathUtil::JoinPath(fence->GetGlobalRoot(), validVersion.GetFenceName());
            indexlib::file_system::MountOption mountOption(indexlib::file_system::FSMT_READ_ONLY);
            mountOption.conflictResolution = indexlib::file_system::ConflictResolution::CHECK_DIFF;
            auto status = fence->GetFileSystem()
                              ->MountVersion(fenceRoot, validVersion.GetVersionId(),
                                             /*logicalPath=*/"", mountOption, nullptr)
                              .Status();
            RETURN_IF_STATUS_ERROR(status, "mount import version[%d] fence[%s] failed", validVersion.GetVersionId(),
                                   validVersion.GetFenceName().c_str());
            mountedVersionIds.insert(validVersion.GetVersionId());
            otherSegDescs.push_back(validVersion.GetSegmentDescriptions());
            auto importVersionLocator = validVersion.GetLocator();
            for (auto [segmentId, schemaId] : validVersion) {
                if (baseVersion->HasSegment(segmentId)) {
                    continue;
                }
                auto segDir = root->GetDirectory(validVersion.GetSegmentDirName(segmentId),
                                                 /*throwExceptionIfNotExist=*/false);
                if (!segDir) {
                    auto status = Status::IOError("get segment[%d] dir failed", segmentId);
                    AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
                    return status;
                }
                auto [status, segLocator] = GetSegmentLocator(segDir->GetIDirectory());
                RETURN_IF_STATUS_ERROR(status, "get segment[%d] locator failed", segmentId);
                if (!importVersionLocator.IsSameSrc(segLocator, false)) {
                    // segment src必须和version src保持一致才会被import进去
                    //这个表明，这个segment是和这个version同源的，需要被import进去
                    //如果导入和version src不同源的segment，会出现bug，比如：
                    //全量segment 0和增量segment 1不同源
                    //全量segment 0和1被merge成2后，如果重复import旧的全量segment 0，依然会被import进来
                    continue;
                }
                framework::Locator baseLocator = baseVersion->GetLocator();
                if (!baseLocator.IsSameSrc(segLocator, false)) {
                    auto importStrategy = options.GetImportStrategy();
                    if (importStrategy == KEEP_SEGMENT_IGNORE_LOCATOR ||
                        importStrategy == KEEP_SEGMENT_OVERWRITE_LOCATOR) {
                        baseVersion->AddSegment(segmentId, schemaId);
                        newSegments.insert(segmentId);
                    } else {
                        AUTIL_LOG(ERROR, "segment locator src not same with base version locator src");
                        return Status::InternalError();
                    }
                } else {
                    if (baseLocator.IsFasterThan(segLocator, false) !=
                        framework::Locator::LocatorCompareResult::LCR_FULLY_FASTER) {
                        baseVersion->AddSegment(segmentId, schemaId);
                        newSegments.insert(segmentId);
                    } else {
                        AUTIL_LOG(INFO, "filter segment, fence:%s segId:%d", validVersion.GetFenceName().c_str(),
                                  segmentId);
                    }
                }
            }
            otherSegDescs.push_back(validVersion.GetSegmentDescriptions());
            std::string versionFileName =
                VERSION_FILE_NAME_PREFIX + std::string(".") + std::to_string(validVersion.GetVersionId());
            indexlib::file_system::RemoveOption removeOption;
            removeOption.logicalDelete = true;
            status = fence->GetFileSystem()->RemoveFile(versionFileName, removeOption).Status();
            if (!status.IsOK()) {
                AUTIL_LOG(ERROR, "logical remove file %s failed", versionFileName.c_str());
                return status;
            } else {
                AUTIL_LOG(INFO, "logical remove file %s", versionFileName.c_str());
            }
            baseVersion->MergeDescriptions(validVersion);
        }

        // 4 import Segment Descriptions
        auto baseSegDescs = baseVersion->GetSegmentDescriptions();
        if (baseSegDescs == nullptr) {
            auto status = Status::InternalError("base segment descriptions is nullptr");
            AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
            return status;
        }
        auto status = baseSegDescs->Import(otherSegDescs, newSegments);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "segment descriptions import failed");
            return status;
        }
        if (resultMaxTs > baseVersion->GetTimestamp()) {
            baseVersion->SetTimestamp(resultMaxTs);
        }
        // TODO(yonghao.fyh) : consider partial faster
        baseVersion->SetLocator(resultLocator);
    }
    if (!removeVersionRet) {
        return Status::IOError();
    }
    return Status::OK();
}

} // namespace indexlibv2::table
