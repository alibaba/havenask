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
#ifndef __INDEXLIB_ATTRIBUTE_UPDATE_BITMAP_H
#define __INDEXLIB_ATTRIBUTE_UPDATE_BITMAP_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/attribute_update_info.h"
#include "indexlib/index/normal/attribute/segment_update_bitmap.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/SimplePool.h"

DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetricsNode);
namespace indexlib { namespace index {

class AttributeUpdateBitmap
{
private:
    struct SegmentBaseDocIdInfo {
        SegmentBaseDocIdInfo() : segId(INVALID_SEGMENTID), baseDocId(INVALID_DOCID) {}

        SegmentBaseDocIdInfo(segmentid_t segmentId, docid_t docId) : segId(segmentId), baseDocId(docId) {}

        segmentid_t segId;
        docid_t baseDocId;
    };

    typedef std::vector<SegmentBaseDocIdInfo> SegBaseDocIdInfoVect;
    typedef std::vector<SegmentUpdateBitmapPtr> SegUpdateBitmapVec;

public:
    AttributeUpdateBitmap() : mTotalDocCount(0), mBuildResourceMetricsNode(NULL) {}

    ~AttributeUpdateBitmap() {}

public:
    void Init(const index_base::PartitionDataPtr& partitionData,
              util::BuildResourceMetrics* buildResourceMetrics = NULL);

    void Set(docid_t globalDocId);

    SegmentUpdateBitmapPtr GetSegmentBitmap(segmentid_t segId)
    {
        for (size_t i = 0; i < mDumpedSegmentBaseIdVec.size(); ++i) {
            if (segId == mDumpedSegmentBaseIdVec[i].segId) {
                return mSegUpdateBitmapVec[i];
            }
        }
        return SegmentUpdateBitmapPtr();
    }

    void Dump(const file_system::DirectoryPtr& dir) const;

private:
    int32_t GetSegmentIdx(docid_t globalDocId) const;
    AttributeUpdateInfo GetUpdateInfoFromBitmap() const;

private:
    util::SimplePool mPool;
    SegBaseDocIdInfoVect mDumpedSegmentBaseIdVec;
    SegUpdateBitmapVec mSegUpdateBitmapVec;
    docid_t mTotalDocCount;
    util::BuildResourceMetricsNode* mBuildResourceMetricsNode;

private:
    IE_LOG_DECLARE();
};

inline int32_t AttributeUpdateBitmap::GetSegmentIdx(docid_t globalDocId) const
{
    if (globalDocId >= mTotalDocCount) {
        return -1;
    }

    size_t i = 1;
    for (; i < mDumpedSegmentBaseIdVec.size(); ++i) {
        if (globalDocId < mDumpedSegmentBaseIdVec[i].baseDocId) {
            break;
        }
    }
    return int32_t(i) - 1;
}

DEFINE_SHARED_PTR(AttributeUpdateBitmap);
}} // namespace indexlib::index

#endif //__INDEXLIB_ATTRIBUTE_UPDATE_BITMAP_H
