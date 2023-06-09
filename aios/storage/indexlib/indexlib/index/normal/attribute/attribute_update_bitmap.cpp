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
#include "indexlib/index/normal/attribute/attribute_update_bitmap.h"
// #include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_define.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

using namespace std;
using namespace autil;

using namespace indexlib::index_base;
using namespace indexlib::util;
using namespace indexlib::file_system;
// using namespace indexlib::file_system;
namespace indexlib { namespace index {
IE_LOG_SETUP(index, AttributeUpdateBitmap);

void AttributeUpdateBitmap::Init(const PartitionDataPtr& partitionData,
                                 util::BuildResourceMetrics* buildResourceMetrics)
{
    if (buildResourceMetrics) {
        mBuildResourceMetricsNode = buildResourceMetrics->AllocateNode();
        IE_LOG(INFO, "allocate build resource metrics node [%d] for AttributeUpdate bitmap",
               mBuildResourceMetricsNode->GetNodeId());
    }
    PartitionData::Iterator iter = partitionData->Begin();
    mTotalDocCount = 0;
    for (; iter != partitionData->End(); iter++) {
        const SegmentData& segmentData = *iter;
        segmentid_t segmentId = segmentData.GetSegmentId();
        docid_t baseDocId = segmentData.GetBaseDocId();
        uint32_t docCount = segmentData.GetSegmentInfo()->docCount;
        mDumpedSegmentBaseIdVec.push_back(SegmentBaseDocIdInfo(segmentId, baseDocId));
        mTotalDocCount += docCount;
        SegmentUpdateBitmapPtr segUpdateBitmap;
        if (docCount > 0) {
            segUpdateBitmap.reset(new SegmentUpdateBitmap(docCount, &mPool));
        }
        mSegUpdateBitmapVec.push_back(segUpdateBitmap);
    }

    if (mBuildResourceMetricsNode) {
        mBuildResourceMetricsNode->Update(util::BMT_CURRENT_MEMORY_USE, mPool.getUsedBytes());
    }
}

void AttributeUpdateBitmap::Set(docid_t globalDocId)
{
    int32_t idx = GetSegmentIdx(globalDocId);
    if (idx == -1) {
        return;
    }
    const SegmentBaseDocIdInfo& segBaseIdInfo = mDumpedSegmentBaseIdVec[idx];
    docid_t localDocId = globalDocId - segBaseIdInfo.baseDocId;
    SegmentUpdateBitmapPtr segUpdateBitmap = mSegUpdateBitmapVec[idx];
    if (!segUpdateBitmap) {
        IE_LOG(ERROR, "fail to set update info for doc:%d", globalDocId);
        return;
    }
    segUpdateBitmap->Set(localDocId);
}

AttributeUpdateInfo AttributeUpdateBitmap::GetUpdateInfoFromBitmap() const
{
    AttributeUpdateInfo updateInfo;
    for (size_t i = 0; i < mSegUpdateBitmapVec.size(); ++i) {
        const SegmentUpdateBitmapPtr& segUpdateBitmap = mSegUpdateBitmapVec[i];
        if (segUpdateBitmap) {
            size_t updateDocCount = segUpdateBitmap->GetUpdateDocCount();
            if (updateDocCount > 0) {
                updateInfo.Add(SegmentUpdateInfo(mDumpedSegmentBaseIdVec[i].segId, updateDocCount));
            }
        }
    }
    return updateInfo;
}

void AttributeUpdateBitmap::Dump(const DirectoryPtr& dir) const
{
    assert(dir);
    AttributeUpdateInfo updateInfo = GetUpdateInfoFromBitmap();
    if (updateInfo.Size() > 0) {
        string jsonStr = autil::legacy::ToJsonString(updateInfo);
        dir->Store(ATTRIBUTE_UPDATE_INFO_FILE_NAME, jsonStr, WriterOption::AtomicDump());
    }
}
}} // namespace indexlib::index
