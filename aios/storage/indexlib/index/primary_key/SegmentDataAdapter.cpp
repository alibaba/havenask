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
#include "indexlib/index/primary_key/SegmentDataAdapter.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/Segment.h"

using namespace std;

namespace indexlibv2::index {

void SegmentDataAdapter::Transform(const std::vector<framework::Segment*>& segments,
                                   std::vector<SegmentDataType>& segmentDatas)
{
    docid_t currentDocCount = 0;
    for (auto segment : segments) {
        if (0 == segment->GetSegmentInfo()->docCount) {
            continue;
        }
        SegmentDataType segmentData;
        segmentData._segmentInfo = segment->GetSegmentInfo();
        segmentData._directory = segment->GetSegmentDirectory();
        segmentData._isRTSegment = segment->IsRtSegmentId(segment->GetSegmentId());
        segmentData._segmentId = segment->GetSegmentId();
        segmentData._baseDocId = currentDocCount;
        segmentData._segment = segment;
        currentDocCount += segment->GetSegmentInfo()->docCount;
        segmentDatas.push_back(segmentData);
    }
}

} // namespace indexlibv2::index
