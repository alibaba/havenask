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
#include <queue>

#include "indexlib/framework/Segment.h"

namespace indexlibv2 { namespace table {

struct NormalTableMergeParam {
protected:
    bool ParseUint32Param(const std::string& kvPair, std::string& keyName, uint32_t& value);
};

struct InputLimits : public NormalTableMergeParam {
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
    // 最大参与merge的segment的doc数目，即segment中doc数目大于max-doc-count的将不参与merge。
    uint32_t maxDocCount;
    // 最大有效文档阀值，当某个的segment的有效文档数大于最大有效文档阀值时不会做merge，默认是uint32_t最大值。
    uint32_t maxValidDocCount;
    // 单位为MB，最大参与merge的segment索引大小，即size大于max-segment-size的segment将不参与merge。
    uint64_t maxSegmentSize;
    // 单位为MB，最大有效索引大小阀值，有效大小是指某个segment中未被delete的文档所占的索引大小（根据validDocCount估算）
    // 当某个segment的有效大小大于max-valid-segment-size时将不参与merge。
    uint64_t maxValidSegmentSize;
    // 单位为MB，参与merge的segment总大小限制，本次merge所有参与merge的segments的总大小将不会大于max-total-merge-size。
    uint64_t maxTotalMergeSize;

private:
    AUTIL_LOG_DECLARE();
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

// 指定优先级队列中segment的优先顺序feature类型，格式为FEATURE_NAME#SEQUENCE。
// FEATURE_NAME内置支持:
// doc-count(doc数）
// delete-doc-count(删除doc数）
// valid-doc-count(总doc数-删除doc数）
// timestamp（时间戳）四种.
// SEQUENCE支持asc（升序优先,默认升序）和desc（降序优先）两种。该参数不指定时默认为doc-count#asc
struct PriorityDescription {
    PriorityDescription() : type(FE_DOC_COUNT), isAsc(true) {}

    void FromString(const std::string& str, bool* isPriorityDescription, bool* isValid);
    std::string ToString() const;

private:
    bool DecodeIsAsc(const std::string& str);

public:
    PriorityType type;
    bool isAsc;
};

struct MergeTriggerDescription : public NormalTableMergeParam {
    MergeTriggerDescription() : conflictSegmentCount(std::numeric_limits<uint32_t>::max()), conflictDeletePercent(100)
    {
    }
    void FromString(const std::string& str, bool* isMergeTriggerDescription, bool* isValid);
    std::string ToString() const;

public:
    // 当优先级队列中的segment的数目大于冲突因子时会触发merge
    uint32_t conflictSegmentCount;
    // 删除文档比例阀值，当某个segment的删除文档比例大于删除文档比例阀值时会触发merge，取值范围是（0-100），默认是100。
    uint32_t conflictDeletePercent;
};

// Example OutputLimits format:
// "max-merged-segment-doc-count=500000;max-total-merge-doc-count;max-merged-segment-size=1024"
struct OutputLimits : public NormalTableMergeParam {
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
    // merge生成的segment中最大支持的docCount总数，单个mergePlan中如果超过该阈值将拆分为多个mergePlan.
    // 如果该参数大于max-total-merge-doc-count，会重置为max-total-merge-doc-count
    uint32_t maxMergedSegmentDocCount;
    // 本次merge最多支持merge的docCount总数，按照merge后的docCount对mergePlan排序.
    // 从docCount最大的mergePlan开始添加生成mergeTask
    uint32_t maxTotalMergedDocCount;
    // 单位为MB，本次merge生成的segment中，单个segment的大小不超过此值。
    uint64_t maxMergedSegmentSize;
    // 单位为MB，本次merge生成的segments总大小不超过此值。
    uint64_t maxTotalMergedSize;

private:
    AUTIL_LOG_DECLARE();
};

struct QueueItem {
    std::shared_ptr<framework::Segment> srcSegment;
    int64_t feature;
    bool isAsc;
};

struct QueueItemComparator {
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
        return left.srcSegment->GetSegmentId() > right.srcSegment->GetSegmentId();
    }
};

}} // namespace indexlibv2::table
