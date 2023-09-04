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
#include "indexlib/index_base/patch/patch_file_finder.h"

#include <algorithm>

#include "autil/StringUtil.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_define.h"
#include "indexlib/util/PathUtil.h"

using namespace autil;
using namespace fslib;

using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::util;
using namespace indexlib::config;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, PatchFileFinder);

segmentid_t PatchFileFinder::ExtractSegmentIdFromDeletionMapFile(const std::string& dataFileName)
{
    size_t pos = dataFileName.find('_');
    if (pos == std::string::npos) {
        return INVALID_SEGMENTID;
    }

    std::string segmentIdStr = dataFileName.substr(pos + 1);
    return GetSegmentIdFromStr(segmentIdStr);
}

bool PatchFileFinder::IsAttrPatchFile(const std::string& dataFileName)
{
    segmentid_t srcSegment;
    segmentid_t destSegment;
    return ExtractSegmentIdFromPatchFile(dataFileName, &srcSegment, &destSegment);
}

bool PatchFileFinder::ExtractSegmentIdFromPatchFile(const std::string& dataFileName, segmentid_t* srcSegment,
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
    StringUtil::fromString(subStr, segmentIdStrs, "_");

    if (segmentIdStrs.size() == 2) {
        *srcSegment = GetSegmentIdFromStr(segmentIdStrs[0]);
        *destSegment = GetSegmentIdFromStr(segmentIdStrs[1]);
        return *srcSegment != INVALID_SEGMENTID && *destSegment != INVALID_SEGMENTID;
    }
    return false;
}

segmentid_t PatchFileFinder::GetSegmentIdFromStr(const std::string& segmentIdStr)
{
    segmentid_t segId = INVALID_SEGMENTID;
    if (!StringUtil::fromString(segmentIdStr, segId)) {
        return INVALID_SEGMENTID;
    }
    return segId;
}

void PatchFileFinder::FindAttrPatchFiles(const AttributeConfigPtr& attrConfig, PatchInfos* patchInfos)
{
    assert(attrConfig);
    PackAttributeConfig* packAttrConfig = attrConfig->GetPackAttributeConfig();
    if (packAttrConfig) {
        FindPatchFiles(PatchType::ATTRIBUTE, packAttrConfig->GetPackName() + "/" + attrConfig->GetAttrName(),
                       INVALID_SCHEMA_OP_ID, patchInfos);
    } else {
        FindPatchFiles(PatchType::ATTRIBUTE, attrConfig->GetAttrName(), attrConfig->GetOwnerModifyOperationId(),
                       patchInfos);
    }
}

void PatchFileFinder::FindAttrPatchFilesForTargetSegment(const config::AttributeConfigPtr& attrConfig,
                                                         segmentid_t targetSegmentId, PatchFileInfoVec* patchInfo)
{
    assert(attrConfig);
    PackAttributeConfig* packAttrConfig = attrConfig->GetPackAttributeConfig();
    if (packAttrConfig) {
        FindPatchFilesForTargetSegment(PatchType::ATTRIBUTE,
                                       packAttrConfig->GetPackName() + "/" + attrConfig->GetAttrName(), targetSegmentId,
                                       INVALID_SCHEMA_OP_ID, patchInfo);
    } else {
        FindPatchFilesForTargetSegment(PatchType::ATTRIBUTE, attrConfig->GetAttrName(), targetSegmentId,
                                       attrConfig->GetOwnerModifyOperationId(), patchInfo);
    }
}

void PatchFileFinder::FindAttrPatchFilesFromSegment(const config::AttributeConfigPtr& attrConfig,
                                                    segmentid_t fromSegmentId, PatchInfos* patchInfos)
{
    assert(attrConfig);
    PackAttributeConfig* packAttrConfig = attrConfig->GetPackAttributeConfig();
    if (packAttrConfig) {
        FindPatchFilesFromSegment(PatchType::ATTRIBUTE, packAttrConfig->GetPackName() + "/" + attrConfig->GetAttrName(),
                                  fromSegmentId, INVALID_SCHEMA_OP_ID, patchInfos);
    } else {
        FindPatchFilesFromSegment(PatchType::ATTRIBUTE, attrConfig->GetAttrName(), fromSegmentId,
                                  attrConfig->GetOwnerModifyOperationId(), patchInfos);
    }
}

void PatchFileFinder::FindIndexPatchFiles(const config::IndexConfigPtr& indexConfig, PatchInfos* patchInfos)
{
    FindPatchFiles(PatchType::INDEX, indexConfig->GetIndexName(), INVALID_SCHEMA_OP_ID, patchInfos);
}

void PatchFileFinder::FindIndexPatchFilesForTargetSegment(const config::IndexConfigPtr& indexConfig,
                                                          segmentid_t targetSegmentId, PatchFileInfoVec* patchInfo)
{
    FindPatchFilesForTargetSegment(PatchType::INDEX, indexConfig->GetIndexName(), targetSegmentId, INVALID_SCHEMA_OP_ID,
                                   patchInfo);
}

void PatchFileFinder::FindIndexPatchFilesFromSegment(const config::IndexConfigPtr& indexConfig,
                                                     segmentid_t fromSegmentId, PatchInfos* patchInfos)
{
    FindPatchFilesFromSegment(PatchType::INDEX, indexConfig->GetIndexName(), fromSegmentId, INVALID_SCHEMA_OP_ID,
                              patchInfos);
}
}} // namespace indexlib::index_base
