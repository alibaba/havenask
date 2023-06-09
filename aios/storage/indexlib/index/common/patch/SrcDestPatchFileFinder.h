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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/index/common/patch/PatchFileFinder.h"

namespace indexlibv2::index {

// 形如 [srcSegmentId]_[destSegmentId].patch 文件名格式的Patch文件
// Attribute和InvertedIndex使用该类型的Patch

class SrcDestPatchFileFinder : public PatchFileFinder
{
public:
    SrcDestPatchFileFinder() = default;
    ~SrcDestPatchFileFinder() = default;

public:
    Status FindPatchFiles(const std::shared_ptr<indexlib::file_system::IDirectory>& indexFieldDir, segmentid_t,
                          PatchInfos* patchInfos) const override;

private:
    static bool AddNewPatchFile(segmentid_t destSegId, const indexlibv2::index::PatchFileInfo& fileInfo,
                                PatchInfos* patchInfos);

    static bool ExtractSegmentIdFromPatchFile(const std::string& dataFileName, segmentid_t* srcSegment,
                                              segmentid_t* destSegment);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
