#include <autil/StringUtil.h>
#include "indexlib/index/normal/attribute/accessor/dedup_patch_file_merger.h"
#include "indexlib/index_base/patch/patch_file_finder_creator.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_merger.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_merger_factory.h"
#include "indexlib/index/normal/attribute/attribute_update_bitmap.h"
#include "indexlib/util/path_util.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/file_system/file_writer.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DedupPatchFileMerger);

DedupPatchFileMerger::DedupPatchFileMerger(
    const AttributeConfigPtr& attrConfig,
    const AttributeUpdateBitmapPtr& attrUpdateBitmap)
    : mAttributeConfig(attrConfig)
    , mAttrUpdateBitmap(attrUpdateBitmap)
{
}

DedupPatchFileMerger::~DedupPatchFileMerger() 
{
}

void DedupPatchFileMerger::Merge(
        const PartitionDataPtr& partitionData, 
        const SegmentMergeInfos& segMergeInfos,
        const file_system::DirectoryPtr& targetAttrDir)
{
    Version version = partitionData->GetVersion();
    if (version.GetSegmentCount() == segMergeInfos.size())
    {
        //optimize merge no need merge patch file
        return;
    }
    if (segMergeInfos.size() == 0)
    {
        IE_LOG(INFO, "segMergeInfos is empty");
        return;
    }

    AttrPatchInfos patchInfos;
    CollectPatchFiles(mAttributeConfig, partitionData, segMergeInfos, patchInfos);
    DoMergePatchFiles(patchInfos, targetAttrDir);
}

void DedupPatchFileMerger::CollectPatchFiles(
    const AttributeConfigPtr& attrConfig,
    const PartitionDataPtr& partitionData,
    const SegmentMergeInfos& segMergeInfos,
    AttrPatchInfos& patchInfos)
{
    PatchFileFinderPtr patchFinder = 
        PatchFileFinderCreator::Create(partitionData.get());
    
    AttrPatchInfos patchesInMergeSegment;
    for (size_t i = 0; i < segMergeInfos.size(); i++)
    {
        segmentid_t segId = segMergeInfos[i].segmentId;
        patchFinder->FindAttrPatchFilesInSegment(
            attrConfig, segId, patchesInMergeSegment);
    }

    AttrPatchInfos allAttrPatches;
    patchFinder->FindAttrPatchFiles(attrConfig, allAttrPatches);

    Version version = partitionData->GetVersion();
    AttrPatchInfos::iterator iter = patchesInMergeSegment.begin();
    for (; iter != patchesInMergeSegment.end(); iter++)
    {
        if (NeedMergePatch(version, segMergeInfos, iter->first))
        {
            GetNeedMergePatchInfos(iter->first, iter->second,
                    allAttrPatches, patchInfos);
        }
    }
}

bool DedupPatchFileMerger::NeedMergePatch(const Version& version,
        const SegmentMergeInfos& segMergeInfos, segmentid_t destSegId)
{
    if (!version.HasSegment(destSegId))
    {
        return false;
    }

    for (size_t i = 0; i < segMergeInfos.size(); i++)
    {
        if (segMergeInfos[i].segmentId == destSegId)
        {
            return false;
        }
    }
    return true;
}

void DedupPatchFileMerger::GetNeedMergePatchInfos(
        segmentid_t destSegment,
        const AttrPatchFileInfoVec& inMergeSegmentPatches,
        const AttrPatchInfos& allPatchInfos,
        AttrPatchInfos& mergePatchInfos)
{
    segmentid_t minSrcSegment = INVALID_SEGMENTID;
    segmentid_t maxSrcSegment = INVALID_SEGMENTID;
    FindSrcSegmentRange(inMergeSegmentPatches, minSrcSegment, maxSrcSegment);

    AttrPatchInfos::const_iterator iter = allPatchInfos.find(destSegment);
    if (iter == allPatchInfos.end())
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "patch file for dest segment[%d] not exist!",
                             destSegment);
    }

    const AttrPatchFileInfoVec& destSegmentPathInfos = iter->second;
    for (size_t i = 0; i < destSegmentPathInfos.size(); i++)
    {
        segmentid_t srcSegment = destSegmentPathInfos[i].srcSegment;
        if (srcSegment >= minSrcSegment && srcSegment <= maxSrcSegment)
        {
            mergePatchInfos[destSegment].push_back(destSegmentPathInfos[i]);
        }
    }
}

void DedupPatchFileMerger::FindSrcSegmentRange(
        const AttrPatchFileInfoVec& patchFiles,
        segmentid_t& minSrcSegId, segmentid_t& maxSrcSegId)
{
    for (size_t i = 0; i < patchFiles.size(); i++)
    {
        if (i == 0)
        {
            minSrcSegId = patchFiles[i].srcSegment;
            maxSrcSegId = patchFiles[i].srcSegment;
            continue;
        }
        
        if (patchFiles[i].srcSegment < minSrcSegId)
        {
            minSrcSegId = patchFiles[i].srcSegment;
            continue;
        }

        if (patchFiles[i].srcSegment > maxSrcSegId)
        {
            maxSrcSegId = patchFiles[i].srcSegment;
        }
    }
}

AttributePatchMergerPtr DedupPatchFileMerger::CreateAttributePatchMerger(segmentid_t segId) const
{
    SegmentUpdateBitmapPtr segUpdateBitmap;
    if (mAttrUpdateBitmap)
    {
        segUpdateBitmap = mAttrUpdateBitmap->GetSegmentBitmap(segId);
    }
    AttributePatchMergerPtr patchMerger(
        AttributePatchMergerFactory::Create(mAttributeConfig, segUpdateBitmap));
    return patchMerger;
}

void DedupPatchFileMerger::DoMergePatchFiles(
        const AttrPatchInfos& patchInfos, 
        const file_system::DirectoryPtr& targetAttrDir)
{
    for (AttrPatchInfos::const_iterator iter = patchInfos.begin();
         iter != patchInfos.end(); ++iter)
    {
        segmentid_t srcSegId = INVALID_SEGMENTID;
        const AttrPatchFileInfoVec& patchInfoVec = iter->second;
        for (size_t i = 0; i < patchInfoVec.size(); ++i)
        {
            if (patchInfoVec[i].srcSegment > srcSegId)
            {
                srcSegId = patchInfoVec[i].srcSegment;
            }
        }
        assert(srcSegId != INVALID_SEGMENTID);
        string patchFileName = StringUtil::toString(srcSegId) + "_" + 
                               StringUtil::toString(iter->first) + "." + 
                               ATTRIBUTE_PATCH_FILE_NAME;
        // string destPatchFilePath = PathUtil::JoinPath(targetAttrDir, patchFileName);
        FileWriterPtr destPatchFileWriter = targetAttrDir->CreateFileWriter(patchFileName);
        AttributePatchMergerPtr patchMerger = CreateAttributePatchMerger(iter->first);
        patchMerger->Merge(iter->second, destPatchFileWriter);
        destPatchFileWriter->Close();
    }
}

IE_NAMESPACE_END(index);
