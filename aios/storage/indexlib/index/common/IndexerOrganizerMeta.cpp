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
#include "indexlib/index/common/IndexerOrganizerMeta.h"

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"

namespace indexlib::index {
namespace {
using indexlibv2::framework::Segment;
}

void IndexerOrganizerMeta::InitIndexerOrganizerMeta(const indexlibv2::framework::TabletData& tabletData,
                                                    IndexerOrganizerMeta* meta)
{
    docid_t baseDocId = 0;
    auto slice = tabletData.CreateSlice();
    for (auto it = slice.begin(); it != slice.end(); it++) {
        Segment* segment = it->get();
        auto segStatus = segment->GetSegmentStatus();
        auto docCount = segment->GetSegmentInfo()->docCount;
        if (segStatus == Segment::SegmentStatus::ST_BUILT && docCount == 0) {
            continue; // there is no document in current segment, so do nothing
        }
        if (!Segment::IsRtSegmentId(segment->GetSegmentId())) {
            meta->realtimeBaseDocId = baseDocId + docCount;
        }
        if (segStatus == Segment::SegmentStatus::ST_BUILT) {
            meta->dumpingBaseDocId = baseDocId + docCount;
            meta->buildingBaseDocId = baseDocId + docCount;
        } else if (segStatus == Segment::SegmentStatus::ST_DUMPING) {
            meta->buildingBaseDocId = baseDocId + docCount;
        }
        baseDocId += docCount;
    }
}

} // namespace indexlib::index
