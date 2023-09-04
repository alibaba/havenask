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
#include "indexlib/table/normal_table/index_task/merger/PriorityQueueMergeStrategy.h"

#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/MergeStrategyDefine.h"
#include "indexlib/table/normal_table/Common.h"
#include "indexlib/table/normal_table/config/SegmentGroupConfig.h"
#include "indexlib/table/normal_table/index_task/merger/NormalTableMergeStrategyUtil.h"

namespace indexlibv2 { namespace table {

namespace {
using indexlibv2::framework::Segment;
using indexlibv2::index::IIndexMerger;
} // namespace

AUTIL_LOG_SETUP(indexlib.table, PriorityQueueMergeStrategy);

std::pair<Status, struct PriorityQueueMergeParams>
PriorityQueueMergeStrategy::ExtractParams(const config::MergeStrategyParameter& config)
{
    struct PriorityQueueMergeParams params;
    if (!params.inputLimits.FromString(config._impl->inputLimitParam)) {
        auto status = Status::InvalidArgs("invalid input limit parameter [%s] for merge strategy",
                                          config._impl->inputLimitParam.c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return {status, params};
    }

    if (!params.outputLimits.FromString(config._impl->outputLimitParam)) {
        auto status = Status::InvalidArgs("invalid output limit parameter [%s] for merge strategy",
                                          config._impl->outputLimitParam.c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return {status, params};
    }

    if (!DecodeStrategyConditions(config._impl->strategyConditions, &(params.priorityDesc),
                                  &(params.mergeTriggerDesc))) {
        auto status = Status::InvalidArgs("invalid merge condition [%s] for merge strategy",
                                          config._impl->strategyConditions.c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return {status, params};
    }
    return {Status::OK(), params};
}

bool PriorityQueueMergeStrategy::DecodeStrategyConditions(const std::string& conditions,
                                                          PriorityDescription* priorityDesc,
                                                          MergeTriggerDescription* mergeTriggerDesc)
{
    std::vector<std::string> mergeConditions;
    autil::StringUtil::fromString(conditions, mergeConditions, ";");
    for (size_t i = 0; i < mergeConditions.size(); i++) {
        bool isPriorityDescription, isValid;
        priorityDesc->FromString(mergeConditions[i], &isPriorityDescription, &isValid);
        if (isPriorityDescription) {
            if (isValid) {
                continue;
            } else {
                return false;
            }
        }
        bool isMergeTriggerDescription;
        mergeTriggerDesc->FromString(mergeConditions[i], &isMergeTriggerDescription, &isValid);
        if (isMergeTriggerDescription) {
            if (isValid) {
                continue;
            } else {
                return false;
            }
        }
        AUTIL_LOG(INFO, "Ignore unknown merge params:%s", mergeConditions[i].c_str());
    }
    return true;
}

Status
PriorityQueueMergeStrategy::CalculateSegmentSizeMap(const std::vector<std::shared_ptr<framework::Segment>>& segments,
                                                    std::map<segmentid_t, size_t>& segmentSizeMap)
{
    segmentSizeMap.clear();
    for (std::shared_ptr<framework::Segment> segment : segments) {
        auto ret = segment->GetSegmentDirectory()->GetIDirectory()->GetDirectorySize(/*path=*/"");
        auto status = ret.Status();
        RETURN_IF_STATUS_ERROR(status, "get directory size fail, dir[%s]",
                               segment->GetSegmentDirectory()->GetLogicalPath().c_str());
        auto segmentSize = ret.result;
        segmentSizeMap[segment->GetSegmentId()] = segmentSize;
    }
    return Status::OK();
}

// PriorityQueueMergeStrategy only works for offline merged segments for now.
// TODO: Maybe revisit this in the future.
std::vector<std::shared_ptr<framework::Segment>>
PriorityQueueMergeStrategy::CollectSrcSegments(const std::shared_ptr<framework::TabletData>& tabletData)
{
    std::vector<std::shared_ptr<framework::Segment>> segments;
    auto slice = tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_BUILT);
    for (auto iter = slice.begin(); iter != slice.end(); iter++) {
        auto segment = *iter;
        if (segment->GetSegmentInfo()->mergedSegment) {
            segments.push_back(segment);
        }
    }
    return segments;
}

std::pair<Status, std::shared_ptr<MergePlan>>
PriorityQueueMergeStrategy::CreateMergePlan(const framework::IndexTaskContext* context)
{
    auto [status, mergePlan] = DoCreateMergePlan(context);
    RETURN2_IF_STATUS_ERROR(status, nullptr, "Create merge plan failed");
    RETURN2_IF_STATUS_ERROR(MergeStrategy::FillMergePlanTargetInfo(context, mergePlan), mergePlan,
                            "fill merge plan target info failed");
    AUTIL_LOG(INFO, "Create merge plan %s", ToJsonString(mergePlan, true).c_str());
    return {Status::OK(), mergePlan};
}

std::pair<Status, std::shared_ptr<MergePlan>>
PriorityQueueMergeStrategy::CreateMergePlanForOneGroup(const framework::IndexTaskContext* context,
                                                       const std::vector<segmentid_t>& candidateSegmentIds)
{
    auto mergeConfig = context->GetMergeConfig();
    if (mergeConfig.GetMergeStrategyStr() == GetName() or
        mergeConfig.GetMergeStrategyStr() == MergeStrategyDefine::COMBINED_MERGE_STRATEGY_NAME) {
        auto [status, params] = ExtractParams(mergeConfig.GetMergeStrategyParameter());
        if (!status.IsOK()) {
            return {status, nullptr};
        }
        _params = std::move(params);
    }
    AUTIL_LOG(INFO, "MergeParams:%s", _params.DebugString().c_str());

    std::vector<std::shared_ptr<framework::Segment>> allSrcSegments = CollectSrcSegments(context->GetTabletData());
    std::vector<std::shared_ptr<framework::Segment>> srcSegments;
    for (const auto& segment : allSrcSegments) {
        if (std::find(candidateSegmentIds.begin(), candidateSegmentIds.end(), segment->GetSegmentId()) !=
            candidateSegmentIds.end()) {
            srcSegments.push_back(segment);
        }
    }

    std::map<segmentid_t, size_t> segmentSizeMap;
    auto status = CalculateSegmentSizeMap(srcSegments, segmentSizeMap);
    RETURN2_IF_STATUS_ERROR(status, nullptr, "calculate segment size map fail");

    std::priority_queue<QueueItem, std::vector<QueueItem>, QueueItemComparator> priorityQueue =
        CreatePriorityQueueFromSrcSegments(srcSegments, segmentSizeMap);

    std::vector<std::shared_ptr<framework::Segment>> needMergeSegments;
    // Filter segments from priority queue using delete percent and conflict segment number so that only segments that
    // match user configuration will be merged
    FilterSegments(segmentSizeMap, &priorityQueue, &needMergeSegments);

    auto mergePlan = std::make_shared<MergePlan>(/*name=*/MERGE_PLAN, /*type=*/MERGE_PLAN);
    uint64_t curTotalMergeDocCount = 0;
    uint64_t curTotalMergedSize = 0;
    while (true) {
        std::vector<std::shared_ptr<framework::Segment>> inPlanSegments;
        ExtractMergeSegmentsForOnePlan(curTotalMergeDocCount, curTotalMergedSize, segmentSizeMap, &needMergeSegments,
                                       &inPlanSegments);
        if (inPlanSegments.empty()) {
            break;
        }

        if (!IsPlanValid(inPlanSegments)) {
            AUTIL_LOG(INFO, "MergePlan has only one segment[%d] and not LargerThanDelPercent!",
                      inPlanSegments[0]->GetSegmentId());
            continue;
        }

        SegmentMergePlan segmentMergePlan;
        for (size_t i = 0; i < inPlanSegments.size(); ++i) {
            segmentMergePlan.AddSrcSegment(inPlanSegments[i]->GetSegmentId());
        }
        mergePlan->AddMergePlan(segmentMergePlan);
        AUTIL_LOG(INFO, "SegmentMergePlan is [%s]", segmentMergePlan.ToString().c_str());
        UpdateMergeTotalStats(inPlanSegments, segmentSizeMap, &curTotalMergeDocCount, &curTotalMergedSize);
    }
    AUTIL_LOG(INFO, "Segment merge plans created.");
    return std::make_pair(Status::OK(), mergePlan);
}

std::pair<Status, std::shared_ptr<MergePlan>>
PriorityQueueMergeStrategy::DoCreateMergePlan(const framework::IndexTaskContext* context)
{
    auto [status, segmentGroupConfig] =
        context->GetTabletSchema()->GetRuntimeSettings().GetValue<SegmentGroupConfig>(NORMAL_TABLE_GROUP_CONFIG_KEY);
    bool enableGroupConfig = false;
    if (!status.IsOK()) {
        if (!status.IsNotFound()) {
            AUTIL_LOG(ERROR, "get segment group config failed");
            return {status, nullptr};
        }
    }
    enableGroupConfig = segmentGroupConfig.IsGroupEnabled();
    std::vector<std::vector<segmentid_t>> groupSegmentIds;
    if (enableGroupConfig) {
        auto groups = segmentGroupConfig.GetGroups();
        groupSegmentIds.resize(groups.size());
        for (const auto& segment : context->GetTabletData()->CreateSlice()) {
            std::string segmentGroup;
            auto [status, segmentStats] = segment->GetSegmentInfo()->GetSegmentStatistics();
            RETURN2_IF_STATUS_ERROR(status, nullptr, "get segment statistics failed");
            [[maybe_unused]] bool isExist = segmentStats.GetStatistic(NORMAL_TABLE_GROUP_TAG_KEY, segmentGroup);
            auto idx = std::find(groups.begin(), groups.end(), segmentGroup) - groups.begin();
            if (idx < groups.size()) {
                groupSegmentIds[idx].push_back(segment->GetSegmentId());
            }
        }
    } else {
        std::vector<segmentid_t> segmentIds;
        for (const auto& segment : context->GetTabletData()->CreateSlice()) {
            segmentIds.push_back(segment->GetSegmentId());
        }
        groupSegmentIds.push_back(segmentIds);
    }
    auto mergePlan = std::make_shared<MergePlan>(/*name=*/MERGE_PLAN, /*type=*/MERGE_PLAN);
    for (const auto& oneGroupSegIds : groupSegmentIds) {
        auto [status, singleMergePlan] = CreateMergePlanForOneGroup(context, oneGroupSegIds);
        RETURN2_IF_STATUS_ERROR(status, nullptr, "create merge plan failed");
        for (int i = 0; i < singleMergePlan->Size(); ++i) {
            mergePlan->AddMergePlan(singleMergePlan->GetSegmentMergePlan(i));
        }
    }
    return {Status::OK(), mergePlan};
}

void PriorityQueueMergeStrategy::ExtractMergeSegmentsForOnePlan(
    const uint64_t curTotalMergeDocCount, const uint64_t curTotalMergedSize,
    const std::map<segmentid_t, uint64_t>& segmentSizeMap,
    std::vector<std::shared_ptr<framework::Segment>>* needMergeSegments,
    std::vector<std::shared_ptr<framework::Segment>>* inPlanSegments)
{
    assert(inPlanSegments->empty());
    std::priority_queue<QueueItem, std::vector<QueueItem>, QueueItemComparator> queue =
        CreatePriorityQueueFromSrcSegments(*needMergeSegments, segmentSizeMap);

    std::vector<std::shared_ptr<framework::Segment>> skipSegments;
    uint64_t inPlanMergeDocCount = 0;
    uint64_t inPlanMergedSize = 0;
    while (!queue.empty()) {
        QueueItem item = queue.top();
        queue.pop();

        uint32_t docCount = item.srcSegment->GetSegmentInfo()->docCount;
        auto result = NormalTableMergeStrategyUtil::GetDeleteDocCount(item.srcSegment.get());
        assert(result.first.IsOK());
        const int64_t deleteDocCount = result.second;
        uint32_t mergeDocCount = GetMergedDocCount(docCount, deleteDocCount);
        uint64_t validSegmentSize =
            GetValidSegmentSize(docCount, deleteDocCount, segmentSizeMap.at(item.srcSegment->GetSegmentId()));
        if (NeedSkipCurrentSegmentByDocCount(curTotalMergeDocCount, inPlanMergeDocCount, mergeDocCount) ||
            NeedSkipCurrentSegmentBySize(curTotalMergedSize, inPlanMergedSize, validSegmentSize)) {
            skipSegments.push_back(item.srcSegment);
            continue;
        }

        inPlanSegments->push_back(item.srcSegment);
        inPlanMergeDocCount += mergeDocCount;
        inPlanMergedSize += validSegmentSize;
    }
    needMergeSegments->swap(skipSegments);
}

uint32_t PriorityQueueMergeStrategy::GetMergedDocCount(uint32_t docCount, int64_t deleteDocCount)
{
    return docCount - deleteDocCount;
}

uint64_t PriorityQueueMergeStrategy::GetValidSegmentSize(uint32_t docCount, int64_t deleteDocCount,
                                                         uint64_t segmentSize)
{
    if (docCount > 0) {
        return segmentSize * (static_cast<double>(GetMergedDocCount(docCount, deleteDocCount)) / docCount);
    }
    return 0;
}

void PriorityQueueMergeStrategy::UpdateMergeTotalStats(
    const std::vector<std::shared_ptr<framework::Segment>>& inPlanSegments,
    const std::map<segmentid_t, uint64_t>& segmentSizeMap, uint64_t* mergedDocCount, uint64_t* mergedSize)
{
    for (size_t i = 0; i < inPlanSegments.size(); ++i) {
        std::shared_ptr<framework::Segment> segment = inPlanSegments.at(i);
        uint32_t docCount = segment->GetSegmentInfo()->docCount;
        auto result = NormalTableMergeStrategyUtil::GetDeleteDocCount(segment.get());
        assert(result.first.IsOK());
        const int64_t deleteDocCount = result.second;

        *mergedDocCount = *mergedDocCount + GetMergedDocCount(segment->GetSegmentInfo()->docCount, deleteDocCount);
        *mergedSize =
            *mergedSize + GetValidSegmentSize(docCount, deleteDocCount, segmentSizeMap.at(segment->GetSegmentId()));
    }
}

bool PriorityQueueMergeStrategy::NeedSkipCurrentSegmentByDocCount(uint64_t curTotalMergeDocCount,
                                                                  uint64_t inPlanMergeDocCount,
                                                                  uint32_t curSegmentValidDocCount)
{
    if (inPlanMergeDocCount + curSegmentValidDocCount > _params.outputLimits.maxMergedSegmentDocCount) {
        return true;
    }

    if (curTotalMergeDocCount + inPlanMergeDocCount + curSegmentValidDocCount >
        _params.outputLimits.maxTotalMergedDocCount) {
        // skip for over total merge doc count
        return true;
    }

    return false;
}

bool PriorityQueueMergeStrategy::NeedSkipCurrentSegmentBySize(uint64_t curTotalMergedSize, uint64_t inPlanMergedSize,
                                                              uint64_t curSegmentValidSize)
{
    if (inPlanMergedSize + curSegmentValidSize > _params.outputLimits.maxMergedSegmentSize) {
        return true;
    }

    if (curTotalMergedSize + inPlanMergedSize + curSegmentValidSize > _params.outputLimits.maxTotalMergedSize) {
        // skip for over total merged size
        return true;
    }

    return false;
}

bool PriorityQueueMergeStrategy::IsPlanValid(const std::vector<std::shared_ptr<framework::Segment>>& inPlanSegments)
{
    if (inPlanSegments.empty()) {
        return false;
    }

    std::shared_ptr<framework::Segment> srcSegment = inPlanSegments[0];
    const uint32_t docCount = srcSegment->GetSegmentInfo()->docCount;
    auto result = NormalTableMergeStrategyUtil::GetDeleteDocCount(srcSegment.get());
    assert(result.first.IsOK());
    const int64_t deleteDocCount = result.second;

    return inPlanSegments.size() > 1 || LargerThanDelPercent(docCount, deleteDocCount);
}

bool PriorityQueueMergeStrategy::LargerThanDelPercent(const uint32_t docCount, const int64_t deleteDocCount)
{
    if (docCount == 0) {
        return false;
    }
    uint32_t curPercent = ((double)deleteDocCount / docCount) * 100;
    return curPercent > _params.mergeTriggerDesc.conflictDeletePercent;
}

void PriorityQueueMergeStrategy::FilterSegments(
    const std::map<segmentid_t, uint64_t>& segmentSizeMap,
    std::priority_queue<QueueItem, std::vector<QueueItem>, QueueItemComparator>* queue,
    std::vector<std::shared_ptr<framework::Segment>>* needMergeSegments)
{
    uint64_t curTotalMergeSize = 0;
    assert(_params.mergeTriggerDesc.conflictSegmentCount > 0);
    while (queue->size() >= _params.mergeTriggerDesc.conflictSegmentCount) {
        for (size_t i = 0; i < _params.mergeTriggerDesc.conflictSegmentCount; i++) {
            QueueItem item = queue->top();
            queue->pop();

            std::shared_ptr<framework::Segment> srcSegment = item.srcSegment;
            const uint64_t segmentSize = segmentSizeMap.at(srcSegment->GetSegmentId());
            if (segmentSize + curTotalMergeSize <= _params.inputLimits.maxTotalMergeSize) {
                needMergeSegments->push_back(item.srcSegment);
                curTotalMergeSize += segmentSize;
            }
        }
    }

    while (!queue->empty()) {
        QueueItem item = queue->top();
        queue->pop();

        std::shared_ptr<framework::Segment> srcSegment = item.srcSegment;
        const uint32_t docCount = srcSegment->GetSegmentInfo()->docCount;
        auto result = NormalTableMergeStrategyUtil::GetDeleteDocCount(srcSegment.get());
        assert(result.first.IsOK());
        const int64_t deleteDocCount = result.second;
        const uint64_t segmentSize = segmentSizeMap.at(srcSegment->GetSegmentId());

        if (LargerThanDelPercent(docCount, deleteDocCount)) {
            if (segmentSize + curTotalMergeSize <= _params.inputLimits.maxTotalMergeSize) {
                needMergeSegments->push_back(item.srcSegment);
                curTotalMergeSize += segmentSize;
            }
        }
    }

    std::stringstream ss;
    for (size_t i = 0; i < needMergeSegments->size(); i++) {
        ss << needMergeSegments->at(i)->GetSegmentId();
        if (i != needMergeSegments->size() - 1) {
            ss << ",";
        }
    }
    AUTIL_LOG(INFO, "candidate merge segments:[%s]", ss.str().c_str());
}

std::priority_queue<QueueItem, std::vector<QueueItem>, QueueItemComparator>
PriorityQueueMergeStrategy::CreatePriorityQueueFromSrcSegments(
    const std::vector<std::shared_ptr<framework::Segment>>& srcSegments,
    const std::map<segmentid_t, uint64_t>& segmentSizeMap)
{
    std::priority_queue<QueueItem, std::vector<QueueItem>, QueueItemComparator> queue;
    for (size_t i = 0; i < srcSegments.size(); i++) {
        std::shared_ptr<framework::Segment> srcSegment = srcSegments[i];
        const segmentid_t segmentId = srcSegment->GetSegmentId();
        const uint32_t docCount = srcSegment->GetSegmentInfo()->docCount;
        const uint64_t segmentSize = segmentSizeMap.at(segmentId);
        auto result = NormalTableMergeStrategyUtil::GetDeleteDocCount(srcSegment.get());
        if (!result.first.IsOK()) {
            continue;
        }
        const int64_t deleteDocCount = result.second;

        if (CheckSegment(segmentId, docCount, deleteDocCount, segmentSize, _params.inputLimits, _params.outputLimits)) {
            queue.push(MakeQueueItem(srcSegment, docCount, deleteDocCount));
        }
    }
    return queue;
}

// Return true if segment is within input and output limit
bool PriorityQueueMergeStrategy::CheckSegment(segmentid_t segmentId, uint32_t docCount, int64_t deleteDocCount,
                                              uint64_t segmentSize, const InputLimits& inputLimits,
                                              const OutputLimits& outputLimits)
{
    if (docCount > inputLimits.maxDocCount) {
        AUTIL_LOG(INFO,
                  "merge ignore segment_[%d]"
                  " for over max-doc-count[%u], docCount[%u]",
                  segmentId, inputLimits.maxDocCount, docCount);
        return false;
    }

    if (segmentSize > inputLimits.maxSegmentSize) {
        AUTIL_LOG(INFO,
                  "merge ignore segment_[%d]"
                  " for over max-segment-size[%lu], segment size[%lu]",
                  segmentId, inputLimits.maxSegmentSize, segmentSize);
        return false;
    }

    const uint32_t validDocCount = docCount - deleteDocCount;
    if (validDocCount > inputLimits.maxValidDocCount) {
        AUTIL_LOG(INFO,
                  "merge ignore segment_[%d]"
                  " for over max-valid-doc-count[%u], validDocCount[%u]",
                  segmentId, inputLimits.maxValidDocCount, validDocCount);
        return false;
    }

    uint64_t validSegmentSize = GetValidSegmentSize(docCount, deleteDocCount, segmentSize);
    if (validSegmentSize > inputLimits.maxValidSegmentSize) {
        AUTIL_LOG(INFO,
                  "merge ignore segment_[%d]"
                  " for over max-valid-segment-size[%lu], valid segment size[%lu]",
                  segmentId, inputLimits.maxValidSegmentSize, validSegmentSize);
        return false;
    }

    // check output limits
    if (validDocCount > outputLimits.maxMergedSegmentDocCount) {
        AUTIL_LOG(INFO,
                  "merge ignore segment_[%d]"
                  " for over max-merged-segment-doc-count[%u], validDocCount[%u]",
                  segmentId, outputLimits.maxMergedSegmentDocCount, validDocCount);
        return false;
    }

    if (validDocCount > outputLimits.maxTotalMergedDocCount) {
        AUTIL_LOG(INFO,
                  "merge ignore segment_[%d]"
                  " for over max-total-merge-doc-count[%u], validDocCount[%u]",
                  segmentId, outputLimits.maxTotalMergedDocCount, validDocCount);
        return false;
    }

    if (validSegmentSize > outputLimits.maxMergedSegmentSize) {
        AUTIL_LOG(INFO,
                  "merge ignore segment_[%d]"
                  " for over max-merged-segment-size[%lu], validSegmentSize[%lu]",
                  segmentId, outputLimits.maxMergedSegmentSize, validSegmentSize);
        return false;
    }

    if (validSegmentSize > outputLimits.maxTotalMergedSize) {
        AUTIL_LOG(INFO,
                  "merge ignore segment_[%d]"
                  " for over max-total-merge-doc-count[%lu], validDocCount[%lu]",
                  segmentId, outputLimits.maxTotalMergedSize, validSegmentSize);
        return false;
    }
    return true;
}

QueueItem PriorityQueueMergeStrategy::MakeQueueItem(std::shared_ptr<framework::Segment> srcSegment, uint32_t docCount,
                                                    int64_t deleteDocCount)
{
    QueueItem item;
    item.srcSegment = srcSegment;
    item.isAsc = _params.priorityDesc.isAsc;
    switch (_params.priorityDesc.type) {
    case FE_DOC_COUNT:
        item.feature = docCount;
        break;
    case FE_DELETE_DOC_COUNT:
        item.feature = deleteDocCount;
        break;
    case FE_VALID_DOC_COUNT:
        item.feature = docCount - deleteDocCount;
        break;
    case FE_TIMESTAMP:
        item.feature = srcSegment->GetSegmentInfo()->timestamp;
        break;
    default:
        assert(false);
        break;
    }
    return item;
}

}} // namespace indexlibv2::table
