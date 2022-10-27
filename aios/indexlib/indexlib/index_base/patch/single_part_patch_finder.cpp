#include <algorithm>
#include <autil/StringUtil.h>
#include "indexlib/index_define.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index_base/patch/single_part_patch_finder.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/util/path_util.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/index_meta/segment_file_meta.h"

using namespace std;
using namespace autil;
using namespace fslib;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, SinglePartPatchFinder);

SinglePartPatchFinder::SinglePartPatchFinder() 
{
}

SinglePartPatchFinder::~SinglePartPatchFinder() 
{
}

void SinglePartPatchFinder::Init(
        const SegmentDirectoryPtr& segmentDirectory)
{
    mSegmentDirectory = segmentDirectory;
}

void SinglePartPatchFinder::FindDeletionMapFiles(DeletePatchInfos& patchInfos)
{
    IE_LOG(DEBUG, "FindDeletionMapFiles begin");
    assert(mSegmentDirectory);
    assert(patchInfos.empty());

    map<string, PatchFileInfo> dataPath;
    FindAllDeletionMapFiles(dataPath);

    Version version = mSegmentDirectory->GetVersion();
    for (map<string, PatchFileInfo>::iterator it = dataPath.begin();
         it != dataPath.end(); ++it)
    {
        const string& dataFileName = it->first;
        segmentid_t segmentId = 
            ExtractSegmentIdFromDeletionMapFile(dataFileName);
        
        if (segmentId == INVALID_SEGMENTID
            || !version.HasSegment(segmentId))
        {
            continue;
        }

        patchInfos[segmentId] = it->second;
    }
    IE_LOG(DEBUG, "FindDeletionMapFiles end");
}

void SinglePartPatchFinder::FindAllDeletionMapFiles(
        map<string, PatchFileInfo>& dataPath)
{
    const SegmentDataVector& segmentDatas = 
        mSegmentDirectory->GetSegmentDatas();
    for (size_t i = 0; i < segmentDatas.size(); i++)
    {
        auto meta = segmentDatas[i].GetSegmentFileMeta();
        DirectoryPtr directory = segmentDatas[i].GetDirectory();
        assert(directory);
        DirectoryPtr deletionmapDirectory = 
            directory->GetDirectory(DELETION_MAP_DIR_NAME, false);

        if (!deletionmapDirectory)
        {
            continue;
        }

        segmentid_t segId = segmentDatas[i].GetSegmentId();

        FileList fileList;

        if (meta)
        {
            meta->ListFile(DELETION_MAP_DIR_NAME, fileList);
        }
        else
        {
            deletionmapDirectory->ListFile("", fileList);
        }
        for (uint32_t i = 0; i < fileList.size(); ++i)
        {
            PatchFileInfo patchInfo;
            patchInfo.srcSegment = segId;
            patchInfo.patchDirectory = deletionmapDirectory;
            patchInfo.patchFileName = fileList[i];
            dataPath[patchInfo.patchFileName] = patchInfo;
        }
    }
}

void SinglePartPatchFinder::FindAttrPatchFiles(
        const string& attrName, schema_opid_t opId, AttrPatchInfos& patchInfos)
{
    IE_LOG(DEBUG, "FindAttrPatchFiles begin");
    patchInfos.clear();
    Version version = mSegmentDirectory->GetVersion();
    for (size_t i = 0; i < version.GetSegmentCount(); i++)
    {
        FindAttrPatchFilesInSegment(attrName, version[i], opId, patchInfos);
    }
    IE_LOG(DEBUG, "FindAttrPatchFiles end");
}

void SinglePartPatchFinder::FindAttrPatchFilesInSegment(
        const string& attrName, segmentid_t segmentId,
        schema_opid_t opId, AttrPatchInfos& patchInfos)
{
    SegmentData segmentData = mSegmentDirectory->GetSegmentData(segmentId);
    if (segmentData.GetSegmentInfo().schemaId < opId)
    {
        IE_LOG(INFO, "segment [%d] will be ignored to find attribute [%s] patch,"
               "segment schemaId [%u] less than attribute owner opId [%u].",
               segmentId, attrName.c_str(),
               segmentData.GetSegmentInfo().schemaId, opId);
        return;
    }
    
    DirectoryPtr attrDirectory = segmentData.GetAttributeDirectory(attrName, false);
    if (!attrDirectory)
    {
        return;
    }
    auto segmentFileMeta = segmentData.GetSegmentFileMeta();
    FileList fileList;
    if (segmentFileMeta)
    {
        segmentFileMeta->ListFile(PathUtil::JoinPath(ATTRIBUTE_DIR_NAME, attrName), fileList);
    }
    else
    {
        attrDirectory->ListFile("", fileList);
    }
    for (uint32_t i = 0; i < fileList.size(); ++i)
    {
        segmentid_t destSegId = INVALID_SEGMENTID;
        segmentid_t srcSegId = INVALID_SEGMENTID;
        if (!ExtractSegmentIdFromAttrPatchFile(
                        fileList[i], srcSegId, destSegId))
        {
            continue;
        }

        PatchFileInfo fileInfo;
        fileInfo.srcSegment = srcSegId;
        fileInfo.patchDirectory = attrDirectory;
        fileInfo.patchFileName = fileList[i];
        
        AddNewAttrPatchFile(destSegId, fileInfo, patchInfos);
    }
}

void SinglePartPatchFinder::AddNewAttrPatchFile(segmentid_t destSegId,
        const PatchFileInfo& fileInfo, AttrPatchInfos& patchInfos)
{
    if (destSegId >= fileInfo.srcSegment)
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, 
                             "Invalid attribute patch file [%s] in [%s]",
                             fileInfo.patchFileName.c_str(),
                             fileInfo.patchDirectory->GetPath().c_str());
    }

    AttrPatchFileInfoVec& fileInfoVec = patchInfos[destSegId];
    AddNewAttrPatchFileInfo(fileInfo, fileInfoVec);
}

void SinglePartPatchFinder::AddNewAttrPatchFileInfo(
        const PatchFileInfo& fileInfo, 
        AttrPatchFileInfoVec& patchInfo)
{
    for (size_t i = 0; i < patchInfo.size(); i++)
    {
        if (fileInfo.srcSegment == patchInfo[i].srcSegment)
        {
            string existPath = PathUtil::JoinPath(
                    patchInfo[i].patchDirectory->GetPath(), 
                    patchInfo[i].patchFileName);
            string curPath = PathUtil::JoinPath(
                    fileInfo.patchDirectory->GetPath(),
                    fileInfo.patchFileName);
            INDEXLIB_FATAL_ERROR(IndexCollapsed, 
                    "duplicate attribute patch files [%s] : [%s]",
                    existPath.c_str(), curPath.c_str());
        }
    }
    patchInfo.push_back(fileInfo);
}

void SinglePartPatchFinder::FindAttrPatchFilesForSegment(
        const string& attrName, segmentid_t segmentId,
        schema_opid_t opId, AttrPatchFileInfoVec& patchInfo)
{
    IE_LOG(DEBUG, "Begin FindAttrPatchFilesForSegment[%d]", segmentId);
    patchInfo.clear();

    assert(mSegmentDirectory);
    const SegmentDataVector& segDataVector = mSegmentDirectory->GetSegmentDatas();
    for (size_t i = 0; i < segDataVector.size(); i++)
    {
        const SegmentData& segmentData = segDataVector[i];
        FindAttrPatchFileInSegment(attrName, segmentData, segmentId, opId, patchInfo);
    }
    IE_LOG(DEBUG, "End FindAttrPatchFilesForSegment[%d]", segmentId);
}

void SinglePartPatchFinder::FindAttrPatchFileInSegment(
        const string& attrName, const SegmentData& segmentData, 
        segmentid_t targetSegmentId, schema_opid_t targetOpId,
        AttrPatchFileInfoVec& patchInfo)
{
    segmentid_t currentSegId = segmentData.GetSegmentId();
    if (segmentData.GetSegmentInfo().schemaId < targetOpId)
    {
        IE_LOG(INFO, "segment [%d] will be ignored to find attribute [%s] patch,"
               "segment schemaId [%u] less than attribute owner opId [%u].",
               currentSegId, attrName.c_str(),
               segmentData.GetSegmentInfo().schemaId, targetOpId);
        return;
    }

    if (currentSegId <= targetSegmentId)
    {
        return;
    }

    if (!segmentData.GetSegmentInfo().mergedSegment)
    {
        FindAttrPatchFileInNotMergedSegment(
                attrName, segmentData, targetSegmentId, targetOpId, patchInfo);
        return;
    }

    // find in merged segment
    AttrPatchInfos patchInfos;
    FindAttrPatchFilesInSegment(attrName, currentSegId, targetOpId, patchInfos);
    AttrPatchFileInfoVec& fileInfoVec = patchInfos[targetSegmentId];
    for (size_t i = 0; i < fileInfoVec.size(); i++)
    {
        AddNewAttrPatchFileInfo(fileInfoVec[i], patchInfo);
    }
}

void SinglePartPatchFinder::FindAttrPatchFileInNotMergedSegment(
        const string& attrName, const SegmentData& segmentData, 
        segmentid_t targetSegmentId, schema_opid_t targetOpId,
        AttrPatchFileInfoVec& patchInfo)
{
    if (segmentData.GetSegmentInfo().schemaId < targetOpId)
    {
        IE_LOG(INFO, "segment [%d] will be ignored to find attribute [%s] patch,"
               "segment schemaId [%u] less than attribute owner opId [%u].",
               segmentData.GetSegmentId(), attrName.c_str(),
               segmentData.GetSegmentInfo().schemaId, targetOpId);
        return;
    }

    DirectoryPtr attrDirectory = segmentData.GetAttributeDirectory(attrName, false);
    if (!attrDirectory)
    {
        return;
    }

    segmentid_t currentSegId = segmentData.GetSegmentId();
    string patchFileName = GetPatchFileName(currentSegId, targetSegmentId);
    if (!attrDirectory->IsExist(patchFileName))
    {
        return;
    }

    PatchFileInfo fileInfo;
    fileInfo.srcSegment = currentSegId;
    fileInfo.patchDirectory = attrDirectory;
    fileInfo.patchFileName = patchFileName;
    AddNewAttrPatchFileInfo(fileInfo, patchInfo);
}

string SinglePartPatchFinder::GetPatchFileName(
        segmentid_t srcSegmentId, segmentid_t dstSegmentId)
{
    stringstream ss;
    ss << srcSegmentId << "_"
       << dstSegmentId << "." << ATTRIBUTE_PATCH_FILE_NAME;
    return ss.str();
}
    
IE_NAMESPACE_END(index_base);

