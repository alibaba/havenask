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
#include "indexlib/index/deletionmap/DeletionMapPatchFileFinder.h"

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/ListOption.h"
#include "indexlib/index/deletionmap/DeletionMapUtil.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, DeletionMapPatchFileFinder);

DeletionMapPatchFileFinder::DeletionMapPatchFileFinder() {}

DeletionMapPatchFileFinder::~DeletionMapPatchFileFinder() {}

Status
DeletionMapPatchFileFinder::FindPatchFiles(const std::shared_ptr<indexlib::file_system::IDirectory>& indexFieldDir,
                                           segmentid_t sourceSegmentId, PatchInfos* patchInfos) const
{
    std::vector<std::string> fileList;
    auto status = indexFieldDir->ListDir("", indexlib::file_system::ListOption(), fileList).Status();
    RETURN_IF_STATUS_ERROR(status, "list dir[%s] failed. ", indexFieldDir->DebugString().c_str());
    for (const auto& file : fileList) {
        auto [isValid, destSegmentId] = DeletionMapUtil::ExtractDeletionMapFileName(file);
        if (!isValid) {
            continue;
        }
        indexlibv2::index::PatchFileInfo fileInfo(sourceSegmentId, destSegmentId, indexFieldDir, file);
        (*patchInfos)[destSegmentId].PushBack(fileInfo);
    }
    return Status::OK();
}

} // namespace indexlibv2::index
