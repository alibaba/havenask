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
#include "indexlib/framework/TabletMemoryCalculator.h"

#include <set>
#include <vector>

#include "indexlib/base/Types.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/TabletReaderContainer.h"
#include "indexlib/framework/Version.h"

namespace indexlibv2::framework {

bool IsIncSegment(const Version& version, segmentid_t segId)
{
    return version.HasSegment(segId) && !Segment::IsPrivateSegmentId(segId);
}

// sum of rt-built segment's memsize
size_t TabletMemoryCalculator::GetRtBuiltSegmentsMemsize() const
{
    std::set<segmentid_t> rtSegmentIdFilter;
    size_t rtBuiltSegmentsMemsize = 0;
    auto tabletDatas = _tabletReaderContainer->GetTabletDatas();
    std::lock_guard<std::mutex> guard(_mutex);
    for (const auto& tabletData : tabletDatas) {
        auto slice = tabletData->CreateSlice(Segment::SegmentStatus::ST_BUILT);
        for (const auto& seg : slice) {
            const segmentid_t segId = seg->GetSegmentId();
            const Version& version = tabletData->GetOnDiskVersion();
            if (IsIncSegment(version, segId)) {
                // current segment is IncBuiltSegment
                continue;
            }
            auto it = rtSegmentIdFilter.find(segId);
            if (it != rtSegmentIdFilter.end()) {
                continue;
            }
            rtSegmentIdFilter.emplace_hint(it, segId);
            rtBuiltSegmentsMemsize += seg->EvaluateCurrentMemUsed();
        }
    }
    return rtBuiltSegmentsMemsize;
}

// sum of inc-built segment's memsize
size_t TabletMemoryCalculator::GetIncIndexMemsize() const
{
    std::set<segmentid_t> incSegmentIdFilter;
    auto tabletDatas = _tabletReaderContainer->GetTabletDatas();
    std::lock_guard<std::mutex> guard(_mutex);
    size_t incBuiltSegmentsMemsize = 0;
    for (const auto& tabletData : tabletDatas) {
        auto slice = tabletData->CreateSlice(Segment::SegmentStatus::ST_BUILT);
        for (const auto& seg : slice) {
            const segmentid_t segId = seg->GetSegmentId();
            const Version& version = tabletData->GetOnDiskVersion();
            if (!IsIncSegment(version, segId)) {
                // current segment is RtBuiltSegment
                continue;
            }
            auto it = incSegmentIdFilter.find(segId);
            if (it != incSegmentIdFilter.end()) {
                continue;
            }
            incSegmentIdFilter.emplace_hint(it, segId);

            incBuiltSegmentsMemsize += seg->EvaluateCurrentMemUsed();
        }
    }
    return incBuiltSegmentsMemsize;
}

size_t TabletMemoryCalculator::GetRtIndexMemsize() const
{
    return GetRtBuiltSegmentsMemsize() + GetDumpingSegmentMemsize() + GetBuildingSegmentMemsize();
}

size_t TabletMemoryCalculator::GetDumpingSegmentMemsize() const
{
    size_t dumpingSegmentsMemsize = 0;
    std::set<segmentid_t> dumpingSegmentIdFilter;
    auto tabletDatas = _tabletReaderContainer->GetTabletDatas();

    std::lock_guard<std::mutex> guard(_mutex);
    if (tabletDatas.empty()) {
        return dumpingSegmentsMemsize;
    }
    // some dumping segments maybe hold by reader in early tabletData
    for (const auto& tabletData : tabletDatas) {
        auto slice = tabletData->CreateSlice(Segment::SegmentStatus::ST_DUMPING);
        for (const auto& seg : slice) {
            const segmentid_t segId = seg->GetSegmentId();
            auto it = dumpingSegmentIdFilter.find(segId);
            if (it != dumpingSegmentIdFilter.end()) {
                continue;
            }
            dumpingSegmentIdFilter.emplace_hint(it, segId);
            dumpingSegmentsMemsize += seg->EvaluateCurrentMemUsed();
        }
    }
    return dumpingSegmentsMemsize;
}

size_t TabletMemoryCalculator::GetBuildingSegmentMemsize() const
{
    std::lock_guard<std::mutex> guard(_mutex);
    return _tabletWriter != nullptr ? _tabletWriter->GetTotalMemSize() : 0;
}
size_t TabletMemoryCalculator::GetBuildingSegmentDumpExpandMemsize() const
{
    std::lock_guard<std::mutex> guard(_mutex);
    return _tabletWriter != nullptr ? _tabletWriter->GetBuildingSegmentDumpExpandSize() : 0;
}

} // namespace indexlibv2::framework
