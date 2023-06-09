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
#include "indexlib/table/index_task/merger/SegmentMergePlan.h"

namespace indexlibv2 { namespace table {

SegmentMergePlan::SegmentMergePlan() {}

SegmentMergePlan::~SegmentMergePlan() {}

void SegmentMergePlan::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("src_segments", _srcSegments, _srcSegments);
    json.Jsonize("target_segments", _targetSegments, _targetSegments);
    json.Jsonize("parameters", _parameters, _parameters);
}

void SegmentMergePlan::UpdateTargetSegmentInfo(segmentid_t sourceSegId,
                                               const std::shared_ptr<framework::TabletData>& tabletData,
                                               framework::SegmentInfo* targetSegInfo)
{
    targetSegInfo->SetLocator(tabletData->GetOnDiskVersion().GetLocator());
    auto segment = tabletData->GetSegment(sourceSegId);
    assert(segment);
    auto segInfo = segment->GetSegmentInfo();
    targetSegInfo->maxTTL = std::max(targetSegInfo->maxTTL, segInfo->maxTTL);
    targetSegInfo->timestamp = std::max(targetSegInfo->timestamp, segInfo->timestamp);
    targetSegInfo->shardCount = segInfo->shardCount;
    // doc count may not right, will be upate later
    // estimate memory use may be used
    targetSegInfo->docCount += segInfo->docCount;
}

}} // namespace indexlibv2::table
