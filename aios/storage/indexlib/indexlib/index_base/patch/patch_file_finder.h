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
#include "indexlib/index_base/patch/patch_file_info.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(config, IndexConfig);

namespace indexlib { namespace index_base {

class PatchFileFinder
{
public:
    enum class PatchType {
        ATTRIBUTE,
        INDEX,
    };

public:
    PatchFileFinder() {}
    virtual ~PatchFileFinder() {}

public:
    virtual void FindDeletionMapFiles(DeletePatchInfos& patchInfos) = 0;

    virtual void FindPatchFiles(PatchType patchType, const std::string& dirName, schema_opid_t opId,
                                PatchInfos* patchInfos) = 0;
    virtual void FindPatchFilesForTargetSegment(PatchType patchType, const std::string& dirName,
                                                segmentid_t targetSegmentId, schema_opid_t opId,
                                                PatchFileInfoVec* patchInfo) = 0;
    virtual void FindPatchFilesFromSegment(PatchType patchType, const std::string& dirName, segmentid_t fromSegmentId,
                                           schema_opid_t opId, PatchInfos* patchInfos) = 0;

public:
    void FindAttrPatchFiles(const config::AttributeConfigPtr& attrConfig, PatchInfos* patchInfos);
    void FindAttrPatchFilesForTargetSegment(const config::AttributeConfigPtr& attrConfig, segmentid_t targetSegmentId,
                                            PatchFileInfoVec* patchInfo);
    void FindAttrPatchFilesFromSegment(const config::AttributeConfigPtr& attrConfig, segmentid_t fromSegmentId,
                                       PatchInfos* patchInfos);

    void FindIndexPatchFiles(const config::IndexConfigPtr& indexConfig, PatchInfos* patchInfos);
    void FindIndexPatchFilesForTargetSegment(const config::IndexConfigPtr& indexConfig, segmentid_t targetSegmentId,
                                             PatchFileInfoVec* patchInfo);
    void FindIndexPatchFilesFromSegment(const config::IndexConfigPtr& indexConfig, segmentid_t fromSegmentId,
                                        PatchInfos* patchInfos);

    static bool IsAttrPatchFile(const std::string& dataFileName);

protected:
    static segmentid_t ExtractSegmentIdFromDeletionMapFile(const std::string& dataFileName);
    static bool ExtractSegmentIdFromPatchFile(const std::string& dataFileName, segmentid_t* srcSegment,
                                              segmentid_t* destSegment);
    static segmentid_t GetSegmentIdFromStr(const std::string& segmentIdStr);

private:
    friend class SinglePartPatchFinderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PatchFileFinder);
}} // namespace indexlib::index_base
