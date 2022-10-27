#include <algorithm>
#include <autil/StringUtil.h>
#include "indexlib/file_system/directory.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/util/path_util.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace autil;
using namespace fslib;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, PatchFileFinder);

bool PatchFileInfo::operator == (const PatchFileInfo& other) const
{
    return srcSegment == other.srcSegment && 
        patchDirectory == other.patchDirectory &&
        patchFileName == other.patchFileName;
}

PatchFileFinder::PatchFileFinder() 
{
}

PatchFileFinder::~PatchFileFinder() 
{
}

segmentid_t PatchFileFinder::ExtractSegmentIdFromDeletionMapFile(
        const string& dataFileName)
{
    size_t pos = dataFileName.find('_');
    if (pos == string::npos)
    {
        return INVALID_SEGMENTID;
    }

    string segmentIdStr = dataFileName.substr(pos + 1);
    return GetSegmentIdFromStr(segmentIdStr);
}       

bool PatchFileFinder::IsAttrPatchFile(const string& dataFileName)
{
    segmentid_t srcSegment;
    segmentid_t destSegment;
    return ExtractSegmentIdFromAttrPatchFile(dataFileName, srcSegment, 
            destSegment);
}

bool PatchFileFinder::ExtractSegmentIdFromAttrPatchFile(
        const string& dataFileName, segmentid_t& srcSegment,
        segmentid_t& destSegment)
{
    srcSegment = INVALID_SEGMENTID;
    destSegment = INVALID_SEGMENTID;

    string suffix = string(".") + ATTRIBUTE_PATCH_FILE_NAME;
    size_t pos = dataFileName.find(suffix);
    if (pos == string::npos || 
        pos + suffix.size() != dataFileName.size())
    {
        return false;
    }

    string subStr = dataFileName.substr(0, pos);
    vector<string> segmentIdStrs;
    StringUtil::fromString(subStr, segmentIdStrs, "_");

    if (segmentIdStrs.size() == 2)
    {
        srcSegment = GetSegmentIdFromStr(segmentIdStrs[0]);
        destSegment = GetSegmentIdFromStr(segmentIdStrs[1]);

        return srcSegment != INVALID_SEGMENTID && 
            destSegment != INVALID_SEGMENTID;
    }
    return false;
}

segmentid_t PatchFileFinder::GetSegmentIdFromStr(const string& segmentIdStr)
{
    segmentid_t segId = INVALID_SEGMENTID;
    if (!StringUtil::fromString(segmentIdStr, segId))
    {
        return INVALID_SEGMENTID;
    }
    return segId;
}

void PatchFileFinder::FindAttrPatchFiles(
    const AttributeConfigPtr& attrConfig,
    AttrPatchInfos& patchInfos)
{
    assert(attrConfig);
    PackAttributeConfig* packAttrConfig =
        attrConfig->GetPackAttributeConfig();
    if (packAttrConfig)
    {
        FindInPackAttrPatchFiles(packAttrConfig->GetAttrName(),
                                 attrConfig->GetAttrName(), patchInfos);
    }
    else
    {
        FindAttrPatchFiles(attrConfig->GetAttrName(),
                           attrConfig->GetOwnerModifyOperationId(), patchInfos);
    }
}

void PatchFileFinder::FindAttrPatchFilesForSegment(
    const AttributeConfigPtr& attrConfig,
    segmentid_t segmentId, AttrPatchFileInfoVec& patchInfo)
{
    assert(attrConfig);
    PackAttributeConfig* packAttrConfig =
        attrConfig->GetPackAttributeConfig();
    if (packAttrConfig)
    {
        FindInPackAttrPatchFilesForSegment(
            packAttrConfig->GetAttrName(), attrConfig->GetAttrName(),
            segmentId, patchInfo);
    }
    else
    {
        FindAttrPatchFilesForSegment(
            attrConfig->GetAttrName(), segmentId,
            attrConfig->GetOwnerModifyOperationId(), patchInfo);
    }
}

void PatchFileFinder::FindAttrPatchFilesInSegment(
    const AttributeConfigPtr& attrConfig,
    segmentid_t segmentId, AttrPatchInfos& patchInfos)
{
    assert(attrConfig);
    PackAttributeConfig* packAttrConfig =
        attrConfig->GetPackAttributeConfig();
    if (packAttrConfig)
    {
        FindInPackAttrPatchFilesInSegment(
                packAttrConfig->GetAttrName(),
                attrConfig->GetAttrName(),
                segmentId, patchInfos);
    }
    else
    {
        FindAttrPatchFilesInSegment(
                attrConfig->GetAttrName(), segmentId,
                attrConfig->GetOwnerModifyOperationId(),
                patchInfos);
    }
}

IE_NAMESPACE_END(index_base);

