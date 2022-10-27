#include "indexlib/index/normal/attribute/accessor/dedup_pack_attribute_patch_file_merger.h"
#include "indexlib/index/normal/attribute/accessor/dedup_patch_file_merger.h"
#include "indexlib/index/normal/attribute/attribute_update_bitmap.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/util/path_util.h"
#include "indexlib/index_base/segment/segment_data.h"

using namespace std;

IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DedupPackAttributePatchFileMerger);

void DedupPackAttributePatchFileMerger::Merge(
    const PartitionDataPtr& partitionData,
    const SegmentMergeInfos& segMergeInfos,
    const DirectoryPtr& targetAttrDir)
{
    AttributeUpdateBitmapPtr packAttrUpdateBitmap(new AttributeUpdateBitmap());
    packAttrUpdateBitmap->Init(partitionData);
    
    const vector<AttributeConfigPtr>& attrConfVec =
        mPackAttrConfig->GetAttributeConfigVec();

    for (size_t i = 0; i < attrConfVec.size(); ++i)
    {
        targetAttrDir->RemoveDirectory(attrConfVec[i]->GetAttrName(), true);
        DirectoryPtr subAttrDir = targetAttrDir->MakeDirectory(attrConfVec[i]->GetAttrName());
        DedupPatchFileMerger subAttrPatchMerger(attrConfVec[i], packAttrUpdateBitmap);
        subAttrPatchMerger.Merge(partitionData, segMergeInfos, subAttrDir);
    }
    packAttrUpdateBitmap->Dump(targetAttrDir);
}

uint32_t DedupPackAttributePatchFileMerger::EstimateMemoryUse(
    const PartitionDataPtr& partitionData,
    const SegmentMergeInfos& segMergeInfos)
{
    const vector<AttributeConfigPtr>& attrConfVec =
        mPackAttrConfig->GetAttributeConfigVec();

    AttrPatchInfos patchInfos;
    for (size_t i = 0; i < attrConfVec.size(); ++i)
    {
        if (!attrConfVec[i]->IsAttributeUpdatable())
        {
            continue;
        }
        DedupPatchFileMerger::CollectPatchFiles(
            attrConfVec[i], partitionData, segMergeInfos, patchInfos);
    }

    int64_t totalDocCount = 0;
    AttrPatchInfos::iterator iter = patchInfos.begin();
    for (; iter != patchInfos.end(); ++iter)
    {
        segmentid_t segId = iter->first;
        totalDocCount += partitionData->GetSegmentData(segId).GetSegmentInfo().docCount;
    }
    return Bitmap::GetDumpSize(totalDocCount);
}


IE_NAMESPACE_END(index);

