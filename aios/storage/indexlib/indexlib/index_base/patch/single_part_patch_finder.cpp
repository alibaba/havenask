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
#include "indexlib/index_base/patch/single_part_patch_finder.h"

#include <algorithm>

#include "autil/StringUtil.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_define.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace fslib;

using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, SinglePartPatchFinder);

SinglePartPatchFinder::SinglePartPatchFinder() {}

SinglePartPatchFinder::~SinglePartPatchFinder() {}

void SinglePartPatchFinder::Init(const SegmentDirectoryPtr& segmentDirectory) { mSegmentDirectory = segmentDirectory; }

void SinglePartPatchFinder::FindDeletionMapFiles(DeletePatchInfos& patchInfos)
{
    IE_LOG(DEBUG, "FindDeletionMapFiles begin");
    assert(mSegmentDirectory);
    assert(patchInfos.empty());

    map<string, PatchFileInfo> dataPath;
    FindAllDeletionMapFiles(dataPath);

    Version version = mSegmentDirectory->GetVersion();
    for (map<string, PatchFileInfo>::iterator it = dataPath.begin(); it != dataPath.end(); ++it) {
        const string& dataFileName = it->first;
        segmentid_t segmentId = ExtractSegmentIdFromDeletionMapFile(dataFileName);

        if (segmentId == INVALID_SEGMENTID || !version.HasSegment(segmentId)) {
            continue;
        }

        patchInfos[segmentId] = it->second;
    }
    IE_LOG(DEBUG, "FindDeletionMapFiles end");
}

void SinglePartPatchFinder::FindAllDeletionMapFiles(map<string, PatchFileInfo>& dataPath)
{
    const SegmentDataVector& segmentDatas = mSegmentDirectory->GetSegmentDatas();
    for (size_t i = 0; i < segmentDatas.size(); i++) {
        DirectoryPtr directory = segmentDatas[i].GetDirectory();
        assert(directory);
        DirectoryPtr deletionmapDirectory = directory->GetDirectory(DELETION_MAP_DIR_NAME, false);

        if (!deletionmapDirectory) {
            continue;
        }

        segmentid_t segId = segmentDatas[i].GetSegmentId();

        FileList fileList;

        deletionmapDirectory->ListDir("", fileList);
        for (uint32_t i = 0; i < fileList.size(); ++i) {
            PatchFileInfo patchInfo;
            patchInfo.srcSegment = segId;
            patchInfo.patchDirectory = deletionmapDirectory;
            patchInfo.patchFileName = fileList[i];
            dataPath[patchInfo.patchFileName] = patchInfo;
        }
    }
}

void SinglePartPatchFinder::FindPatchFiles(PatchType patchType, const std::string& dirName, schema_opid_t opId,
                                           PatchInfos* patchInfos)
{
    IE_LOG(DEBUG, "FindPatchFiles begin");
    patchInfos->clear();
    Version version = mSegmentDirectory->GetVersion();
    for (size_t i = 0; i < version.GetSegmentCount(); i++) {
        FindPatchFilesFromSegment(patchType, dirName, version[i], opId, patchInfos);
    }
    IE_LOG(DEBUG, "FindPatchFiles end");
}

void SinglePartPatchFinder::FindPatchFilesForTargetSegment(PatchType patchType, const std::string& dirName,
                                                           segmentid_t targetSegmentId, schema_opid_t opId,
                                                           PatchFileInfoVec* patchInfo)
{
    IE_LOG(DEBUG, "Begin FindPatchFilesForTargetSegment[%d]", targetSegmentId);
    patchInfo->clear();

    assert(mSegmentDirectory);
    const SegmentDataVector& segDataVector = mSegmentDirectory->GetSegmentDatas();
    for (size_t i = 0; i < segDataVector.size(); i++) {
        const SegmentData& segmentData = segDataVector[i];
        FindPatchFileFromSegment(patchType, segmentData, dirName, targetSegmentId, opId, patchInfo);
    }
    IE_LOG(DEBUG, "End FindPatchFilesForTargetSegment[%d]", targetSegmentId);
}

void SinglePartPatchFinder::FindPatchFilesFromSegment(PatchType patchType, const std::string& dirName,
                                                      segmentid_t fromSegmentId, schema_opid_t opId,
                                                      PatchInfos* patchInfos)
{
    SegmentData segmentData = mSegmentDirectory->GetSegmentData(fromSegmentId);
    if (segmentData.GetSegmentInfo()->schemaId < opId) {
        IE_LOG(INFO,
               "segment [%d] will be ignored to find [%s] patch,"
               "segment schemaId [%u] less than owner opId [%u].",
               fromSegmentId, dirName.c_str(), segmentData.GetSegmentInfo()->schemaId, opId);
        return;
    }

    DirectoryPtr dir = nullptr;
    if (patchType == PatchType::ATTRIBUTE) {
        dir = segmentData.GetAttributeDirectory(dirName, false);
    } else if (patchType == PatchType::INDEX) {
        dir = segmentData.GetIndexDirectory(dirName, false);
    } else {
        assert(false);
    }
    if (dir == nullptr) {
        return;
    }

    FileList fileList;
    dir->ListDir("", fileList);
    for (uint32_t i = 0; i < fileList.size(); ++i) {
        segmentid_t destSegId = INVALID_SEGMENTID;
        segmentid_t srcSegId = INVALID_SEGMENTID;
        if (!ExtractSegmentIdFromPatchFile(fileList[i], &srcSegId, &destSegId)) {
            continue;
        }

        PatchFileInfo fileInfo;
        fileInfo.srcSegment = srcSegId;
        fileInfo.destSegment = destSegId;
        fileInfo.patchDirectory = dir;
        fileInfo.patchFileName = fileList[i];

        AddNewPatchFile(patchType, destSegId, fileInfo, patchInfos);
    }
}

void SinglePartPatchFinder::AddNewPatchFile(PatchType patchType, segmentid_t destSegId, const PatchFileInfo& fileInfo,
                                            PatchInfos* patchInfos)
{
    if (destSegId > fileInfo.srcSegment) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Invalid patch file [%s] in [%s]", fileInfo.patchFileName.c_str(),
                             fileInfo.patchDirectory->DebugString().c_str());
    }
    if (destSegId == fileInfo.srcSegment and patchType == PatchType::ATTRIBUTE) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Invalid patch file [%s] in [%s]", fileInfo.patchFileName.c_str(),
                             fileInfo.patchDirectory->DebugString().c_str());
    }
    if (destSegId != fileInfo.destSegment) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Invalid patch file [%s] in [%s]", fileInfo.patchFileName.c_str(),
                             fileInfo.patchDirectory->DebugString().c_str());
    }
    PatchFileInfoVec* fileInfoVec = &((*patchInfos)[destSegId]);
    AddNewPatchFileInfo(fileInfo, fileInfoVec);
}

void SinglePartPatchFinder::AddNewPatchFileInfo(const PatchFileInfo& fileInfo, PatchFileInfoVec* patchInfo)
{
    for (size_t i = 0; i < patchInfo->size(); i++) {
        const PatchFileInfo& patchFileInfo = (*patchInfo)[i];
        if (fileInfo.srcSegment == patchFileInfo.srcSegment) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "duplicate patch files [%s] : [%s]",
                                 patchFileInfo.patchDirectory->DebugString(patchFileInfo.patchFileName).c_str(),
                                 fileInfo.patchDirectory->DebugString(fileInfo.patchFileName).c_str());
        }
    }
    patchInfo->push_back(fileInfo);
}

void SinglePartPatchFinder::FindPatchFileFromSegment(PatchType patchType, const SegmentData& fromSegmentData,
                                                     const std::string& dirName, segmentid_t targetSegmentId,
                                                     schema_opid_t targetOpId, PatchFileInfoVec* patchInfo)
{
    segmentid_t currentSegId = fromSegmentData.GetSegmentId();
    if (fromSegmentData.GetSegmentInfo()->schemaId < targetOpId) {
        IE_LOG(INFO,
               "segment [%d] will be ignored to find [%s] patch,"
               "segment schemaId [%u] less than owner opId [%u].",
               currentSegId, dirName.c_str(), fromSegmentData.GetSegmentInfo()->schemaId, targetOpId);
        return;
    }

    if (currentSegId < targetSegmentId) {
        return;
    }

    if (!fromSegmentData.GetSegmentInfo()->mergedSegment) {
        FindPatchFileFromNotMergedSegment(patchType, fromSegmentData, dirName, targetSegmentId, targetOpId, patchInfo);
        return;
    }

    // find in merged segment
    PatchInfos patchInfos;
    FindPatchFilesFromSegment(patchType, dirName, currentSegId, targetOpId, &patchInfos);
    PatchFileInfoVec& fileInfoVec = patchInfos[targetSegmentId];
    for (size_t i = 0; i < fileInfoVec.size(); i++) {
        AddNewPatchFileInfo(fileInfoVec[i], patchInfo);
    }
}

void SinglePartPatchFinder::FindPatchFileFromNotMergedSegment(PatchType patchType, const SegmentData& fromSegmentData,
                                                              const std::string& dirName, segmentid_t targetSegmentId,
                                                              schema_opid_t targetOpId, PatchFileInfoVec* patchInfo)
{
    if (fromSegmentData.GetSegmentInfo()->schemaId < targetOpId) {
        IE_LOG(INFO,
               "segment [%d] will be ignored to find [%s] patch,"
               "segment schemaId [%u] less than owner opId [%u].",
               fromSegmentData.GetSegmentId(), dirName.c_str(), fromSegmentData.GetSegmentInfo()->schemaId, targetOpId);
        return;
    }

    DirectoryPtr dir = nullptr;
    if (patchType == PatchType::ATTRIBUTE) {
        dir = fromSegmentData.GetAttributeDirectory(dirName, false);
    } else if (patchType == PatchType::INDEX) {
        dir = fromSegmentData.GetIndexDirectory(dirName, false);
    } else {
        assert(false);
    }

    if (!dir) {
        return;
    }

    segmentid_t currentSegId = fromSegmentData.GetSegmentId();
    string patchFileName = GetPatchFileName(currentSegId, targetSegmentId);
    if (!dir->IsExist(patchFileName)) {
        return;
    }

    PatchFileInfo fileInfo;
    fileInfo.srcSegment = currentSegId;
    fileInfo.destSegment = targetSegmentId;
    fileInfo.patchDirectory = dir;
    fileInfo.patchFileName = patchFileName;
    AddNewPatchFileInfo(fileInfo, patchInfo);
}

string SinglePartPatchFinder::GetPatchFileName(segmentid_t srcSegmentId, segmentid_t dstSegmentId)
{
    stringstream ss;
    ss << srcSegmentId << "_" << dstSegmentId << "." << PATCH_FILE_NAME;
    return ss.str();
}
}} // namespace indexlib::index_base
