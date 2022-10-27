#include "indexlib/index/normal/attribute/attribute_update_bitmap.h"
// #include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/path_util.h"
#include "indexlib/util/memory_control/build_resource_metrics.h"
#include "indexlib/index_base/segment/segment_data.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(file_system);
// IE_NAMESPACE_USE(storage);
IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AttributeUpdateBitmap);

void AttributeUpdateBitmap::Init(const PartitionDataPtr& partitionData,
                                 util::BuildResourceMetrics* buildResourceMetrics)
{
    if (buildResourceMetrics)
    {
        mBuildResourceMetricsNode = buildResourceMetrics->AllocateNode();
        IE_LOG(INFO, "allocate build resource metrics node [%d] for AttributeUpdate bitmap",
               mBuildResourceMetricsNode->GetNodeId());
    }
    PartitionData::Iterator iter = partitionData->Begin();
    mTotalDocCount = 0;
    for ( ; iter != partitionData->End(); iter++)
    {
        const SegmentData& segmentData = *iter;
        segmentid_t segmentId = segmentData.GetSegmentId();
        docid_t baseDocId = segmentData.GetBaseDocId();
        uint32_t docCount = segmentData.GetSegmentInfo().docCount;
        mDumpedSegmentBaseIdVec.push_back(
            SegmentBaseDocIdInfo(segmentId, baseDocId));
        mTotalDocCount += docCount;
        SegmentUpdateBitmapPtr segUpdateBitmap;
        if (docCount > 0)
        {
            segUpdateBitmap.reset(
                new SegmentUpdateBitmap(docCount, &mPool));
        }
        mSegUpdateBitmapVec.push_back(segUpdateBitmap);
    }

    if (mBuildResourceMetricsNode)
    {
        mBuildResourceMetricsNode->Update(util::BMT_CURRENT_MEMORY_USE,
                mPool.getUsedBytes());
    }
}

void AttributeUpdateBitmap::Set(docid_t globalDocId)
{
    int32_t idx = GetSegmentIdx(globalDocId);
    if (idx == -1)
    {
        return;
    }
    const SegmentBaseDocIdInfo &segBaseIdInfo = mDumpedSegmentBaseIdVec[idx];
    docid_t localDocId = globalDocId - segBaseIdInfo.baseDocId;
    SegmentUpdateBitmapPtr segUpdateBitmap = mSegUpdateBitmapVec[idx];
    if (!segUpdateBitmap)
    {
        IE_LOG(ERROR, "fail to set update info for doc:%d", globalDocId);
        return;
    }
    segUpdateBitmap->Set(localDocId);
}

AttributeUpdateInfo AttributeUpdateBitmap::GetUpdateInfoFromBitmap() const
{
    AttributeUpdateInfo updateInfo;
    for (size_t i = 0 ; i < mSegUpdateBitmapVec.size(); ++i)
    {
        const SegmentUpdateBitmapPtr& segUpdateBitmap = mSegUpdateBitmapVec[i];
        if (segUpdateBitmap)
        {
            size_t updateDocCount = segUpdateBitmap->GetUpdateDocCount();
            if (updateDocCount > 0)
            {
                updateInfo.Add(
                    SegmentUpdateInfo(
                        mDumpedSegmentBaseIdVec[i].segId, updateDocCount));
            }
        }
    }
    return updateInfo;
}

void AttributeUpdateBitmap::Dump(const DirectoryPtr& dir) const
{
    assert(dir);
    AttributeUpdateInfo updateInfo = GetUpdateInfoFromBitmap();
    if (updateInfo.Size() > 0)
    {
        string jsonStr = autil::legacy::ToJsonString(updateInfo);
        dir->Store(ATTRIBUTE_UPDATE_INFO_FILE_NAME, jsonStr, true);
    }
}

// void AttributeUpdateBitmap::Dump(const std::string& dir) const
// {
//     AttributeUpdateInfo updateInfo = GetUpdateInfoFromBitmap();
//     if (updateInfo.Size() > 0)
//     {
//         string jsonStr = autil::legacy::ToJsonString(updateInfo);
//         string filePath = PathUtil::JoinPath(dir, ATTRIBUTE_UPDATE_INFO_FILE_NAME);
//         FileSystemWrapper::AtomicStore(filePath, jsonStr);
//     }
// }


IE_NAMESPACE_END(index);

