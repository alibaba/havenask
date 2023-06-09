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
#include "indexlib/index/normal/primarykey/segment_data_adapter.h"

#include "indexlib/framework/Segment.h"
#include "indexlib/index_base/segment/built_segment_iterator.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/segment_data.h"

using namespace std;
using namespace indexlib::index_base;

namespace indexlib::index {

void SegmentDataAdapter::Transform(const indexlib::index_base::PartitionDataPtr& partitionData,
                                   std::vector<indexlibv2::index::SegmentDataAdapter::SegmentDataType>& segmentDatas)
{
    indexlib::index_base::PartitionSegmentIteratorPtr segIter = partitionData->CreateSegmentIterator();
    assert(segIter);
    indexlib::index_base::SegmentIteratorPtr builtSegIter = segIter->CreateIterator(SIT_BUILT);
    while (builtSegIter->IsValid()) {
        indexlibv2::index::SegmentDataAdapter::SegmentDataType segmentData;
        SegmentData metaSegData = builtSegIter->GetSegmentData();
        segmentData._segmentInfo = metaSegData.GetSegmentInfo();

        if (0 == segmentData._segmentInfo->docCount) {
            builtSegIter->MoveToNext();
            continue;
        }

        segmentData._isRTSegment = indexlibv2::framework::Segment::IsRtSegmentId(metaSegData.GetSegmentId());
        segmentData._directory = metaSegData.GetDirectory();
        segmentData._segmentId = metaSegData.GetSegmentId();
        segmentData._baseDocId = metaSegData.GetBaseDocId();
        segmentDatas.push_back(segmentData);
        builtSegIter->MoveToNext();
    }
}

} // namespace indexlib::index
