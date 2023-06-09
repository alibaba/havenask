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
#include "indexlib/index/common/patch/PatchFileFinder.h"

#include "indexlib/config/IIndexConfig.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/ListOption.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, PatchFileFinder);

Status PatchFileFinder::FindAllPatchFiles(const std::vector<std::shared_ptr<framework::Segment>>& segments,
                                          const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                          std::map<segmentid_t, PatchFileInfos>* patchInfos) const
{
    for (auto srcSegment : segments) {
        auto segDir = srcSegment->GetSegmentDirectory()->GetIDirectory();
        assert(segDir != nullptr);
        auto [status, indexDir] = GetIndexDirectory(segDir, indexConfig);
        RETURN_STATUS_DIRECTLY_IF_ERROR(status);
        if (indexDir != nullptr) {
            auto status = FindPatchFiles(indexDir, srcSegment->GetSegmentId(), patchInfos);
            RETURN_IF_STATUS_ERROR(status, "get patch file fail for [%s] [%s] in dir [%s].",
                                   indexConfig->GetIndexType().c_str(), indexConfig->GetIndexName().c_str(),
                                   indexDir->GetLogicalPath().c_str());
        }
    }
    for (const auto& segment : segments) {
        if (patchInfos->find(segment->GetSegmentId()) != patchInfos->end()) {
            AUTIL_LOG(INFO, "for segment [%d] index [%s] find patch files [%s]", segment->GetSegmentId(),
                      indexConfig->GetIndexName().c_str(),
                      patchInfos->at(segment->GetSegmentId()).DebugString().c_str());
        }
    }
    return Status::OK();
}

segmentid_t PatchFileFinder::GetSegmentIdFromStr(const std::string& segmentIdStr)
{
    segmentid_t segId = INVALID_SEGMENTID;
    if (!autil::StringUtil::fromString(segmentIdStr, segId)) {
        return INVALID_SEGMENTID;
    }
    return segId;
}

std::pair<Status, std::shared_ptr<indexlib::file_system::IDirectory>>
PatchFileFinder::GetIndexDirectory(const std::shared_ptr<indexlib::file_system::IDirectory>& segDir,
                                   const std::shared_ptr<config::IIndexConfig>& indexConfig) const
{
    std::vector<std::string> indexPaths = indexConfig->GetIndexPath();
    // PackageIndexConfig might return multiple indexPaths, i.e. index+section attribute
    if (indexPaths.size() > 1) {
        assert(indexPaths.size() == 2);
        auto packageIndexConfig = std::dynamic_pointer_cast<config::PackageIndexConfig>(indexConfig);
        assert(packageIndexConfig != nullptr);
        indexPaths = std::vector<std::string> {indexPaths[0]};
    }
    if (indexPaths.size() == 0) {
        auto ic = std::dynamic_pointer_cast<config::InvertedIndexConfig>(indexConfig);
        assert(ic != nullptr);
        assert(ic->GetShardingType() == config::InvertedIndexConfig::IST_NEED_SHARDING);
        return {Status::OK(), nullptr};
    }
    assert(indexPaths.size() == 1);
    auto [ec, indexDir] = segDir->GetDirectory(indexPaths[0]);
    if (ec == indexlib::file_system::ErrorCode::FSEC_NOENT) {
        return {Status::OK(), nullptr};
    }
    if (ec != indexlib::file_system::ErrorCode::FSEC_OK) {
        return {toStatus(ec), nullptr};
    }
    return {Status::OK(), indexDir};
}

} // namespace indexlibv2::index
