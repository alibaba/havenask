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
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/common/patch/PatchFileInfos.h"

namespace indexlibv2::index {
class PatchMerger;

class PatchFileMerger : private autil::NoCopyable
{
public:
    PatchFileMerger(const PatchInfos& patchInfos) : _allPatchInfos(patchInfos) {}
    virtual ~PatchFileMerger() = default;

public:
    Status Merge(const IIndexMerger::SegmentMergeInfos& segMergeInfos);

    Status CollectPatchFiles(const IIndexMerger::SegmentMergeInfos& segMergeInfos, PatchInfos* patchInfos);

private:
    bool NeedMergePatch(const IIndexMerger::SegmentMergeInfos& segMergeInfos, segmentid_t destSegId);
    Status GetNeedMergePatchInfos(segmentid_t destSegId, const PatchFileInfos& inMergeSegmentPatches,
                                  PatchInfos* mergePatchInfos);
    void FindSrcSegmentRange(const PatchFileInfos& patchFiles, segmentid_t* minSrcSegId, segmentid_t* maxSrcSegId);
    Status DoMergePatchFiles(const PatchInfos& patchInfos,
                             const std::shared_ptr<indexlib::file_system::IDirectory>& targetSegmentDir);

private:
    virtual std::shared_ptr<PatchMerger> CreatePatchMerger(segmentid_t segId) const = 0;
    virtual Status FindPatchFiles(const IIndexMerger::SegmentMergeInfos& segMergeInfos, PatchInfos* patchInfos) = 0;

protected:
    const PatchInfos& _allPatchInfos;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
