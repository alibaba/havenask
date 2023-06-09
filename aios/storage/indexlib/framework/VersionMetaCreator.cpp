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
#include "indexlib/framework/VersionMetaCreator.h"

#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/framework/DeployIndexUtil.h"
#include "indexlib/framework/TabletData.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, VersionMetaCreator);

std::pair<Status, VersionMeta>
VersionMetaCreator::Create(const std::shared_ptr<indexlib::file_system::Directory>& rootDirectory,
                           const TabletData& tabletData, const Version& version)
{
    uint64_t docCount = 0;
    std::vector<segmentid_t> missingSegments;
    for (auto [segId, _] : version) {
        auto segment = tabletData.GetSegment(segId);
        if (segment) {
            docCount += segment->GetSegmentInfo()->docCount;
        } else {
            missingSegments.push_back(segId);
        }
    }
    auto getSegmentDocCount = [&version](const std::shared_ptr<indexlib::file_system::Directory>& rootDir,
                                         segmentid_t segId) -> std::optional<uint64_t> {
        auto segDir = rootDir->GetDirectory(version.GetSegmentDirName(segId), /*throwExceptionIfNotExist*/ false);
        if (!segDir) {
            return std::optional<uint64_t>();
        }
        SegmentInfo segInfo;
        auto readerOption = indexlib::file_system::ReaderOption::NoCache(indexlib::file_system::FSOT_MEM);
        readerOption.forceRemotePath = true;
        auto status = segInfo.Load(segDir->GetIDirectory(), readerOption);
        if (!status.IsOK()) {
            return std::optional<uint64_t>();
        }
        return segInfo.docCount;
    };
    if (!missingSegments.empty()) {
        for (auto segId : missingSegments) {
            auto segDoc = getSegmentDocCount(rootDirectory, segId);
            if (!segDoc) {
                return {Status::InternalError("get segment doc count failed"), VersionMeta()};
            }
            docCount += segDoc.value();
        }
    }
    auto outputRoot = rootDirectory->GetFileSystem()->GetOutputRootPath();
    auto [status, indexSize] = DeployIndexUtil::GetIndexSize(outputRoot, version.GetVersionId());
    if (!status.IsOK()) {
        return {status, VersionMeta()};
    }
    return {Status::OK(), VersionMeta(version, static_cast<int64_t>(docCount), indexSize)};
}

} // namespace indexlibv2::framework
