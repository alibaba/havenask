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

#include "indexlib/common_define.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index_base, InMemorySegmentReader);
DECLARE_REFERENCE_CLASS(index, SegmentDirectoryBase);

namespace indexlib { namespace index_base {

struct DocRangeInfo {
    exdocid_t mBeginDocId;
    exdocid_t mEndDocId;
    DocRangeInfo()
    {
        mBeginDocId = 0;
        mEndDocId = 0;
    }
    DocRangeInfo(exdocid_t beginDocId, exdocid_t endDocId)
    {
        mBeginDocId = beginDocId;
        mEndDocId = endDocId;
    }
};

struct SegmentMergeInfo : public autil::legacy::Jsonizable {
    SegmentMergeInfo()
        : segmentId(INVALID_SEGMENTID)
        , deletedDocCount(0)
        , baseDocId(0)
        , segmentSize(0)
        , levelIdx(0)
        , inLevelIdx(0)
    {
    }

    // Attention:
    // baseId should be the base doc id in segmentMergeInfos,
    // started from 0.
    SegmentMergeInfo(segmentid_t segId, const SegmentInfo& segInfo, uint32_t delDocCount, exdocid_t baseId,
                     int64_t segSize = 0, uint32_t levelId = 0, uint32_t inLevelId = 0)
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

    segmentid_t segmentId;
    uint32_t deletedDocCount;
    exdocid_t baseDocId;
    int64_t segmentSize;
    SegmentInfo segmentInfo;
    uint32_t levelIdx;
    uint32_t inLevelIdx;
    indexlib::framework::SegmentMetrics segmentMetrics;
    // do not need to serialize
    file_system::DirectoryPtr directory;
    index::SegmentDirectoryBasePtr segmentDirectory;
    // TODO: legacy for reclaim map unittest
    index_base::InMemorySegmentReaderPtr TEST_inMemorySegmentReader;
};

typedef std::vector<SegmentMergeInfo> SegmentMergeInfos;
}} // namespace indexlib::index_base
