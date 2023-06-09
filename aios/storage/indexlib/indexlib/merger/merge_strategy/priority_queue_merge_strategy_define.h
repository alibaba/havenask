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
#ifndef __INDEXLIB_PRIORITY_QUEUE_MERGE_STRATEGY_DEFINE_H
#define __INDEXLIB_PRIORITY_QUEUE_MERGE_STRATEGY_DEFINE_H

#include <memory>
#include <queue>

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace merger {

struct PriorityMergeParam {
protected:
    bool ParseUint32Param(const std::string& kvPair, std::string& keyName, uint32_t& value);
};

struct InputLimits : public PriorityMergeParam {
    InputLimits()
        : maxDocCount(std::numeric_limits<uint32_t>::max())
        , maxValidDocCount(std::numeric_limits<uint32_t>::max())
        , maxSegmentSize(std::numeric_limits<uint64_t>::max())
        , maxValidSegmentSize(std::numeric_limits<uint64_t>::max())
        , maxTotalMergeSize(std::numeric_limits<uint64_t>::max())
    {
    }
    bool FromString(const std::string& inputStr);
    std::string ToString() const;

public:
    uint32_t maxDocCount;
    uint32_t maxValidDocCount;
    uint64_t maxSegmentSize;
    uint64_t maxValidSegmentSize;
    uint64_t maxTotalMergeSize;
};

enum PriorityType {
    FE_INVALID,
    FE_DOC_COUNT,
    FE_DELETE_DOC_COUNT,
    FE_VALID_DOC_COUNT,
    FE_TIMESTAMP,
    FE_DOC_TEMPERTURE_DIFFERENCE,
    // FE_SEGMENT_SIZE
};

struct PriorityDescription {
    PriorityDescription() : type(FE_DOC_COUNT), isAsc(true) {}

    bool FromString(const std::string& str);
    std::string ToString() const;

private:
    bool DecodeIsAsc(const std::string& str);

public:
    PriorityType type;
    bool isAsc;
};

struct MergeTriggerDescription : public PriorityMergeParam {
    MergeTriggerDescription() : conflictSegmentCount(std::numeric_limits<uint32_t>::max()), conflictDeletePercent(100)
    {
    }
    bool FromString(const std::string& str);
    std::string ToString() const;

public:
    uint32_t conflictSegmentCount;
    uint32_t conflictDeletePercent;
};

//"max-merged-segment-doc-count=500000;max-total-merge-doc-count;max-merged-segment-size=1024"
struct OutputLimits : public PriorityMergeParam {
    OutputLimits()
        : maxMergedSegmentDocCount(std::numeric_limits<uint32_t>::max())
        , maxTotalMergedDocCount(std::numeric_limits<uint32_t>::max())
        , maxMergedSegmentSize(std::numeric_limits<uint64_t>::max())
        , maxTotalMergedSize(std::numeric_limits<uint64_t>::max())
    {
    }
    bool FromString(const std::string& str);
    std::string ToString() const;

public:
    uint32_t maxMergedSegmentDocCount;
    uint32_t maxTotalMergedDocCount;
    uint64_t maxMergedSegmentSize;
    uint64_t maxTotalMergedSize;

private:
    IE_LOG_DECLARE();
};

struct QueueItem {
    index_base::SegmentMergeInfo mergeInfo;
    int64_t feature;
    bool isAsc;
};

struct QueueItemCompare {
    bool operator()(const QueueItem& left, const QueueItem& right)
    {
        int64_t leftFeature = left.isAsc ? left.feature : right.feature;
        int64_t rightFeature = left.isAsc ? right.feature : left.feature;

        if (leftFeature > rightFeature) {
            return true;
        }
        if (leftFeature < rightFeature) {
            return false;
        }
        return left.mergeInfo.segmentId > right.mergeInfo.segmentId;
    }
};

typedef std::priority_queue<QueueItem, std::vector<QueueItem>, QueueItemCompare> PriorityQueue;
}} // namespace indexlib::merger

#endif //__INDEXLIB_PRIORITY_QUEUE_MERGE_STRATEGY_DEFINE_H
