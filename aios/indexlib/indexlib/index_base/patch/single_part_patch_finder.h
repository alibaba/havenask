#ifndef __INDEXLIB_SINGLE_PART_PATCH_FINDER_H
#define __INDEXLIB_SINGLE_PART_PATCH_FINDER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/patch/patch_file_finder.h"

DECLARE_REFERENCE_CLASS(index_base, SegmentDirectory);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);

IE_NAMESPACE_BEGIN(index_base);

class SinglePartPatchFinder : public PatchFileFinder
{
public:
    SinglePartPatchFinder();
    ~SinglePartPatchFinder();

public:
    void Init(const SegmentDirectoryPtr& segmentDirectory);

    void FindDeletionMapFiles(
            DeletePatchInfos& patchInfos) override;

    void FindAttrPatchFiles(
            const std::string& attrName, schema_opid_t opId,
            AttrPatchInfos& patchInfos) override;

    void FindAttrPatchFilesForSegment(
            const std::string& attrName, segmentid_t segmentId, 
            schema_opid_t opId, AttrPatchFileInfoVec& patchInfo) override;

    void FindAttrPatchFilesInSegment(
            const std::string& attrName, segmentid_t segmentId,
            schema_opid_t opId, AttrPatchInfos& patchInfos) override;

private:
    void FindAllDeletionMapFiles(
            std::map<std::string, PatchFileInfo>& dataPath);

    void AddNewAttrPatchFile(segmentid_t destSegId, 
                             const PatchFileInfo& fileInfo, 
                             AttrPatchInfos& patchInfos);

    void AddNewAttrPatchFileInfo(const PatchFileInfo& fileInfo, 
                                 AttrPatchFileInfoVec& patchInfo);

    void FindAttrPatchFileInSegment(const std::string& attrName,
                                    const SegmentData& segData,
                                    segmentid_t targetSegmentId,
                                    schema_opid_t targetOpId,
                                    AttrPatchFileInfoVec& patchInfo);

    void FindAttrPatchFileInNotMergedSegment(const std::string& attrName,
            const SegmentData& segData, segmentid_t targetSegmentId,
            schema_opid_t targetOpId, AttrPatchFileInfoVec& patchInfo);

    std::string GetPatchFileName(segmentid_t srcSegmentId, 
                                 segmentid_t dstSegmentId);

private:
    SegmentDirectoryPtr mSegmentDirectory;

private:
    friend class SinglePartPatchFinderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SinglePartPatchFinder);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_SINGLE_PART_PATCH_FINDER_H
