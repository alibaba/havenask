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
#include "indexlib/framework/cleaner/DropIndexCleaner.h"

#include "indexlib/base/PathUtil.h"
#include "indexlib/framework/Fence.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, DropIndexCleaner);

DropIndexCleaner::DropIndexCleaner() {}

DropIndexCleaner::~DropIndexCleaner() {}

std::vector<std::string>
DropIndexCleaner::CaclulateDropIndexDirs(const std::shared_ptr<TabletData>& originTabletData,
                                         const std::shared_ptr<config::TabletSchema>& targetSchema)
{
    std::vector<std::string> dropIndexDirs;
    auto slice = originTabletData->CreateSlice();
    for (auto segment : slice) {
        auto segmentDir = segment->GetSegmentDirectory()->GetLogicalPath();
        auto segmentSchema = segment->GetSegmentSchema();
        auto indexConfigs = segmentSchema->GetIndexConfigs();
        for (auto indexConfig : indexConfigs) {
            if (!targetSchema->GetIndexConfig(indexConfig->GetIndexType(), indexConfig->GetIndexName())) {
                auto paths = indexConfig->GetIndexPath();
                dropIndexDirs.insert(dropIndexDirs.end(), paths.begin(), paths.end());
            }
        }
    }
    return dropIndexDirs;
}

Status DropIndexCleaner::CleanIndexInLogical(const std::shared_ptr<TabletData>& originTabletData,
                                             const std::shared_ptr<config::TabletSchema>& targetSchema,
                                             const std::shared_ptr<indexlib::file_system::IDirectory>& rootDirectory)
{
    auto slice = originTabletData->CreateSlice();
    auto removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    removeOption.logicalDelete = true;
    for (auto segment : slice) {
        auto segmentSchema = segment->GetSegmentSchema();
        if (segmentSchema->GetSchemaId() >= targetSchema->GetSchemaId()) {
            continue;
        }
        auto [status, segmentDir] =
            rootDirectory->GetDirectory(segment->GetSegmentDirectory()->GetLogicalPath()).StatusWith();
        RETURN_IF_STATUS_ERROR(status, "get segment dir failed");
        auto indexConfigs = segmentSchema->GetIndexConfigs();
        for (auto indexConfig : indexConfigs) {
            if (!targetSchema->GetIndexConfig(indexConfig->GetIndexType(), indexConfig->GetIndexName())) {
                auto paths = indexConfig->GetIndexPath();
                for (auto indexPath : paths) {
                    auto status = segmentDir->RemoveDirectory(indexPath, removeOption).Status();
                    AUTIL_LOG(INFO, "logical remove directory [%s]",
                              (segmentDir->GetLogicalPath() + "/" + indexPath).c_str());
                    RETURN_IF_STATUS_ERROR(status, "remove logical directory failed");
                }
            }
        }
    }
    return Status::OK();
}

Status DropIndexCleaner::DropPrivateFence(const std::string& localRoot)
{
    std::string fenceName = Fence::GetPrivateFenceName();
    std::string localFenceRoot = PathUtil::JoinPath(localRoot, fenceName);
    indexlib::file_system::RemoveOption removeOption;
    removeOption.mayNonExist = true;
    auto localDir = indexlib::file_system::Directory::GetPhysicalDirectory(localRoot);
    return localDir->GetIDirectory()->RemoveDirectory(fenceName, removeOption).Status();
}

} // namespace indexlibv2::framework
