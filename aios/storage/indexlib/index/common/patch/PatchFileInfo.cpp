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
#include "indexlib/index/common/patch/PatchFileInfo.h"

#include "indexlib/base/PathUtil.h"
#include "indexlib/file_system/IDirectory.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, PatchFileInfo);

PatchFileInfo::PatchFileInfo(segmentid_t _srcSegment, segmentid_t _destSegment,
                             std::shared_ptr<indexlib::file_system::IDirectory> _patchDirectory,
                             const std::string& _patchFileName)
    : srcSegment(_srcSegment)
    , destSegment(_destSegment)
    , patchDirectory(_patchDirectory)
    , patchFullPath(PathUtil::JoinPath(_patchDirectory->GetLogicalPath(), _patchFileName))
    , patchFileName(_patchFileName)
{
}

PatchFileInfo::PatchFileInfo()
    : srcSegment(INVALID_SEGMENTID)
    , destSegment(INVALID_SEGMENTID)
    , patchDirectory(nullptr)
    , patchFullPath("")
    , patchFileName("")
{
}

void PatchFileInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("src_segment", srcSegment, srcSegment);
    json.Jsonize("dest_Segment", destSegment, destSegment);
    json.Jsonize("patch_full_path", patchFullPath, patchFullPath);
}

bool PatchFileInfo::operator==(const PatchFileInfo& other) const
{
    return srcSegment == other.srcSegment and destSegment == other.destSegment and
           patchDirectory == other.patchDirectory and patchFullPath == other.patchFullPath and
           patchFileName == other.patchFileName;
}

} // namespace indexlibv2::index
