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
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index_base, MultiPartSegmentDirectory);

namespace indexlib { namespace index_base {

class MultiPartPatchFinder : public PatchFileFinder
{
public:
    MultiPartPatchFinder();
    ~MultiPartPatchFinder();

public:
    void Init(const MultiPartSegmentDirectoryPtr& segmentDirectory);

    void FindDeletionMapFiles(DeletePatchInfos& patchInfos) override;

    void FindPatchFiles(PatchType patchType, const std::string& dirName, schema_opid_t opId,
                        PatchInfos* patchInfos) override;
    void FindPatchFilesForTargetSegment(PatchType patchType, const std::string& dirName, segmentid_t targetSegmentId,
                                        schema_opid_t opId, PatchFileInfoVec* patchInfo) override;
    void FindPatchFilesFromSegment(PatchType patchType, const std::string& dirName, segmentid_t fromSegmentId,
                                   schema_opid_t opId, PatchInfos* patchInfos) override;

private:
    void AddDeletionMapFiles(size_t partitionIdx, const DeletePatchInfos& singlePartPatchInfos,
                             DeletePatchInfos& multiPartPatchInfos);
    void AddPatchFiles(size_t partitionIdx, const PatchInfos& singlePartPatchInfos, PatchInfos* multiPartPatchInfos);

    void ConvertSegmentIdToVirtual(size_t partitionIdx, PatchFileInfoVec* patchInfoVector);
    void ConvertSegmentIdToVirtual(size_t partitionIdx, PatchFileInfo* patchInfo);

private:
    MultiPartSegmentDirectoryPtr mSegmentDirectory;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiPartPatchFinder);
}} // namespace indexlib::index_base
