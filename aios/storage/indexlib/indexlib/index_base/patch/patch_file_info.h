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

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace index_base {

struct PatchFileInfo {
    segmentid_t srcSegment = INVALID_SEGMENTID;
    segmentid_t destSegment = INVALID_SEGMENTID;
    file_system::DirectoryPtr patchDirectory;
    std::string patchFileName;
    bool operator==(const PatchFileInfo& other) const
    {
        return srcSegment == other.srcSegment && patchDirectory == other.patchDirectory &&
               patchFileName == other.patchFileName;
    }
};

typedef std::vector<PatchFileInfo> PatchFileInfoVec;
typedef std::map<segmentid_t, PatchFileInfoVec> PatchInfos;
typedef std::map<segmentid_t, PatchFileInfo> DeletePatchInfos;
}} // namespace indexlib::index_base
