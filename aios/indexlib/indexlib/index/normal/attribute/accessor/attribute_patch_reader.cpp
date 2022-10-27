#include "indexlib/file_system/directory.h"
#include "indexlib/index_base/patch/patch_file_finder_creator.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_reader.h"

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);

void AttributePatchReader::Init(
        const PartitionDataPtr& partitionData, segmentid_t segmentId)
{
    AttrPatchInfos attrPatchInfos;
    PatchFileFinderPtr patchFinder = PatchFileFinderCreator::Create(partitionData.get());

    patchFinder->FindAttrPatchFiles(mAttrConfig, attrPatchInfos);
    AttrPatchFileInfoVec patchInfoVec = attrPatchInfos[segmentId];
    for (size_t i = 0; i < patchInfoVec.size(); i++)
    {
        AddPatchFile(patchInfoVec[i].patchDirectory, 
                     patchInfoVec[i].patchFileName, 
                     patchInfoVec[i].srcSegment);
    }
}

IE_NAMESPACE_END(index);
