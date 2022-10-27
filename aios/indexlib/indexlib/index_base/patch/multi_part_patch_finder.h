#ifndef __INDEXLIB_MULTI_PART_PATCH_FINDER_H
#define __INDEXLIB_MULTI_PART_PATCH_FINDER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index_base/patch/patch_file_finder.h"

DECLARE_REFERENCE_CLASS(index_base, MultiPartSegmentDirectory);

IE_NAMESPACE_BEGIN(index_base);

class MultiPartPatchFinder : public PatchFileFinder
{
public:
    MultiPartPatchFinder();
    ~MultiPartPatchFinder();
public:
    void Init(const MultiPartSegmentDirectoryPtr& segmentDirectory);

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
    void AddDeletionMapFiles(size_t partitionIdx,
                             const DeletePatchInfos& singlePartPatchInfos,
                             DeletePatchInfos& multiPartPatchInfos);
    void AddAttributePatchFiles(size_t partitionIdx,
                                const AttrPatchInfos& singlePartPatchInfos,
                                AttrPatchInfos& multiPartPatchInfos);
    void ConvertSrcSegmentIdToVirtual(size_t partitionIdx,
            AttrPatchFileInfoVec& patchInfoVector);
    void ConvertSrcSegmentIdToVirtual(size_t partitionIdx, 
            PatchFileInfo& patchInfo);

private:
    MultiPartSegmentDirectoryPtr mSegmentDirectory;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiPartPatchFinder);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_MULTI_PART_PATCH_FINDER_H
