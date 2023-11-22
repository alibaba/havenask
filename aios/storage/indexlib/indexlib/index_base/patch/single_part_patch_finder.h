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

DECLARE_REFERENCE_CLASS(index_base, SegmentDirectory);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);

namespace indexlib { namespace index_base {

class SinglePartPatchFinder : public PatchFileFinder
{
public:
    SinglePartPatchFinder();
    ~SinglePartPatchFinder();

public:
    void Init(const SegmentDirectoryPtr& segmentDirectory);

    void FindDeletionMapFiles(DeletePatchInfos& patchInfos) override;

    void FindPatchFiles(PatchType patchType, const std::string& dirName, schema_opid_t opId,
                        PatchInfos* patchInfos) override;
    void FindPatchFilesForTargetSegment(PatchType patchType, const std::string& dirName, segmentid_t targetSegmentId,
                                        schema_opid_t opId, PatchFileInfoVec* patchInfo) override;
    void FindPatchFilesFromSegment(PatchType patchType, const std::string& dirName, segmentid_t fromSegmentId,
                                   schema_opid_t opId, PatchInfos* patchInfos) override;

private:
    void FindAllDeletionMapFiles(std::map<std::string, PatchFileInfo>& dataPath);

    void FindPatchFiles(const file_system::DirectoryPtr& dir, schema_opid_t opId, PatchInfos* patchInfos);
    void FindPatchFileFromSegment(PatchType patchType, const SegmentData& fromSegmentData, const std::string& dirName,
                                  segmentid_t targetSegmentId, schema_opid_t targetOpId, PatchFileInfoVec* patchInfo);
    void FindPatchFileFromNotMergedSegment(PatchType patchType, const SegmentData& fromSegmentData,
                                           const std::string& dirName, segmentid_t targetSegmentId,
                                           schema_opid_t targetOpId, PatchFileInfoVec* patchInfo);

    void AddNewPatchFile(PatchType patchType, segmentid_t destSegId, const PatchFileInfo& fileInfo,
                         PatchInfos* patchInfos);
    void AddNewPatchFileInfo(const PatchFileInfo& fileInfo, PatchFileInfoVec* patchInfo);

public:
    static std::string GetPatchFileName(segmentid_t srcSegmentId, segmentid_t dstSegmentId);

private:
    SegmentDirectoryPtr mSegmentDirectory;

private:
    friend class SinglePartPatchFinderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SinglePartPatchFinder);
}} // namespace indexlib::index_base
