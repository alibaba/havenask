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
#include "indexlib/index/common/patch/SrcDestPatchFileFinder.h"

#include "indexlib/base/Constant.h"
#include "indexlib/base/Status.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/attribute/Common.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, SrcDestPatchFileFinder);

bool SrcDestPatchFileFinder::ExtractSegmentIdFromPatchFile(const std::string& dataFileName, segmentid_t* srcSegment,
                                                           segmentid_t* destSegment)
{
    *srcSegment = INVALID_SEGMENTID;
    *destSegment = INVALID_SEGMENTID;

    std::string suffix = std::string(".") + PATCH_FILE_NAME;
    size_t pos = dataFileName.find(suffix);
    if (pos == std::string::npos || pos + suffix.size() != dataFileName.size()) {
        return false;
    }

    std::string subStr = dataFileName.substr(0, pos);
    std::vector<std::string> segmentIdStrs;
    autil::StringUtil::fromString(subStr, segmentIdStrs, "_");

    if (segmentIdStrs.size() == 2) {
        *srcSegment = GetSegmentIdFromStr(segmentIdStrs[0]);
        *destSegment = GetSegmentIdFromStr(segmentIdStrs[1]);
        return *srcSegment != INVALID_SEGMENTID && *destSegment != INVALID_SEGMENTID;
    }
    return false;
}

Status SrcDestPatchFileFinder::FindPatchFiles(const std::shared_ptr<indexlib::file_system::IDirectory>& indexFieldDir,
                                              segmentid_t, PatchInfos* patchInfos) const
{
    std::vector<std::string> fileList;
    auto status = indexFieldDir->ListDir("", indexlib::file_system::ListOption(), fileList).Status();
    RETURN_IF_STATUS_ERROR(status, "list dir[%s] failed. ", indexFieldDir->DebugString().c_str());
    for (uint32_t i = 0; i < fileList.size(); ++i) {
        segmentid_t destSegId = INVALID_SEGMENTID;
        segmentid_t srcSegId = INVALID_SEGMENTID;
        if (!ExtractSegmentIdFromPatchFile(fileList[i], &srcSegId, &destSegId)) {
            continue;
        }

        indexlibv2::index::PatchFileInfo fileInfo;
        fileInfo.srcSegment = srcSegId;
        fileInfo.destSegment = destSegId;
        fileInfo.patchDirectory = indexFieldDir;
        fileInfo.patchFileName = fileList[i];
        if (!AddNewPatchFile(destSegId, fileInfo, patchInfos)) {
            return Status::InternalError("add new path info for segment [%d] failed. ", destSegId);
        }
    }
    return Status::OK();
}

bool SrcDestPatchFileFinder::AddNewPatchFile(segmentid_t destSegId, const indexlibv2::index::PatchFileInfo& fileInfo,
                                             PatchInfos* patchInfos)
{
    if (destSegId > fileInfo.srcSegment) {
        AUTIL_LOG(ERROR, "Invalid patch file [%s] in [%s]", fileInfo.patchFileName.c_str(),
                  fileInfo.patchDirectory->DebugString().c_str());
        return false;
    }
    if (destSegId == fileInfo.srcSegment) {
        AUTIL_LOG(ERROR, "Invalid patch file [%s] in [%s]", fileInfo.patchFileName.c_str(),
                  fileInfo.patchDirectory->DebugString().c_str());
        return false;
    }

    auto& patchFileInfos = (*patchInfos)[destSegId];
    for (size_t i = 0; i < patchFileInfos.Size(); ++i) {
        const indexlibv2::index::PatchFileInfo& patchFileInfo = patchFileInfos[i];
        if (fileInfo.srcSegment == patchFileInfo.srcSegment) {
            AUTIL_LOG(ERROR, "duplicate patch files [%s] : [%s]",
                      patchFileInfo.patchDirectory->DebugString(patchFileInfo.patchFileName).c_str(),
                      fileInfo.patchDirectory->DebugString(fileInfo.patchFileName).c_str());
            return false;
        }
    }

    patchFileInfos.PushBack(fileInfo);
    return true;
}

} // namespace indexlibv2::index
