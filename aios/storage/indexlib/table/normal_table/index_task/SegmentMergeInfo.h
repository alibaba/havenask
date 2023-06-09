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
#pragma once
#include <memory>

#include "autil/legacy/json.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/framework/SegmentMetrics.h"

namespace indexlibv2::table {

struct SegmentMergeInfo : public autil::legacy::Jsonizable {
    SegmentMergeInfo() {}

    // Attention:
    // baseId should be the base doc id in segmentMergeInfos,
    // started from 0.
    SegmentMergeInfo(segmentid_t segId, const indexlibv2::framework::SegmentInfo& segInfo, uint32_t delDocCount,
                     exdocid_t baseId, int64_t segSize, uint32_t levelId, uint32_t inLevelId)
        : segmentId(segId)
        , deletedDocCount(delDocCount)
        , baseDocId(baseId)
        , segmentSize(segSize)
        , segmentInfo(segInfo)
        , levelIdx(levelId)
        , inLevelIdx(inLevelId)
    {
    }

    void Jsonize(JsonWrapper& json) override
    {
        json.Jsonize("segment_id", segmentId, segmentId);
        json.Jsonize("deleted_doc_count", deletedDocCount, deletedDocCount);
        json.Jsonize("base_doc_id", baseDocId, baseDocId);
        json.Jsonize("segment_size", segmentSize, segmentSize);
        json.Jsonize("segment_info", segmentInfo, segmentInfo);
        json.Jsonize("levelIdx", levelIdx);
        json.Jsonize("inLevelIdx", inLevelIdx);
        json.Jsonize("segment_metrics", segmentMetrics, segmentMetrics);
    }

    bool operator<(const SegmentMergeInfo& other) const
    {
        if (levelIdx != other.levelIdx) {
            return levelIdx > other.levelIdx;
        }
        return inLevelIdx < other.inLevelIdx;
    }

    segmentid_t segmentId = INVALID_SEGMENTID;
    uint32_t deletedDocCount = 0;
    exdocid_t baseDocId = 0;
    int64_t segmentSize = 0;
    indexlibv2::framework::SegmentInfo segmentInfo;
    uint32_t levelIdx = 0;
    uint32_t inLevelIdx = 0;
    indexlib::framework::SegmentMetrics segmentMetrics;
};

} // namespace indexlibv2::table
