#ifndef __INDEXLIB_DEDUP_PATCH_FILE_MERGER_H
#define __INDEXLIB_DEDUP_PATCH_FILE_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/patch_file_merger.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_merger.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/patch/patch_file_finder.h"

DECLARE_REFERENCE_CLASS(index, AttributeUpdateBitmap);
IE_NAMESPACE_BEGIN(index);

class DedupPatchFileMerger : public PatchFileMerger
{
public:
    typedef AttributePatchMerger::PatchFileList PatchFileList;

public:
    DedupPatchFileMerger(const config::AttributeConfigPtr& attrConfig,
                         const AttributeUpdateBitmapPtr& attrUpdateBitmap
                         = AttributeUpdateBitmapPtr());
    ~DedupPatchFileMerger();
public:
    
    void Merge(const index_base::PartitionDataPtr& partitionData,
               const index_base::SegmentMergeInfos& segMergeInfos,
               const file_system::DirectoryPtr& targetAttrDir) override;

    static void CollectPatchFiles(
        const config::AttributeConfigPtr& attrConfig,
        const index_base::PartitionDataPtr& partitionData,
        const index_base::SegmentMergeInfos& segMergeInfos,
        index_base::AttrPatchInfos& patchInfos);

private:
    void DoMergePatchFiles(const index_base::AttrPatchInfos& patchInfos, 
                           const file_system::DirectoryPtr& targetAttrDir);

    virtual AttributePatchMergerPtr CreateAttributePatchMerger(segmentid_t segId) const;

    static bool NeedMergePatch(const index_base::Version& version, 
                               const index_base::SegmentMergeInfos& segMergeInfos,
                               segmentid_t destSegId);

    static void GetNeedMergePatchInfos(
        segmentid_t destSegment,
        const index_base::AttrPatchFileInfoVec& inMergeSegmentPatches,
        const index_base::AttrPatchInfos& allPatchInfos, 
        index_base::AttrPatchInfos& mergePatchInfos);

    static void FindSrcSegmentRange(const index_base::AttrPatchFileInfoVec& patchFiles,
                                    segmentid_t& minSrcSegId, segmentid_t& maxSrcSegId);
private:
    config::AttributeConfigPtr mAttributeConfig;
    AttributeUpdateBitmapPtr mAttrUpdateBitmap;

private:
    friend class DedupPatchFileMergerTest;
    IE_LOG_DECLARE();

};

DEFINE_SHARED_PTR(DedupPatchFileMerger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DEDUP_PATCH_FILE_MERGER_H
