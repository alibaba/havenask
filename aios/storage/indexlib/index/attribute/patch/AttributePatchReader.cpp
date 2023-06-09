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
#include "indexlib/index/attribute/patch/AttributePatchReader.h"

#include <assert.h>

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, AttributePatchReader);

Status AttributePatchReader::Init(const PatchInfos* patchInfos, segmentid_t segmentId)
{
    auto it = patchInfos->find(segmentId);
    assert(it != patchInfos->end());
    const PatchFileInfos& patchFileInfos = it->second;
    for (size_t i = 0; i < patchFileInfos.Size(); i++) {
        auto status = AddPatchFile(patchFileInfos[i].patchDirectory, patchFileInfos[i].patchFileName,
                                   patchFileInfos[i].srcSegment);
        RETURN_IF_STATUS_ERROR(status, "add patch file failed.");
    }
    return Status::OK();
}

} // namespace indexlibv2::index
