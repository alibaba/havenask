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
#include "indexlib/merger/merge_strategy/priority_queue_merge_strategy.h"

#include <assert.h>
#include <memory>
#include <ostream>
#include <stddef.h>
#include <vector>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"
#include "indexlib/merger/merge_strategy/optimize_merge_strategy.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::index_base;
namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, PriorityQueueMergeStrategy);

PriorityQueueMergeStrategy::PriorityQueueMergeStrategy(const merger::SegmentDirectoryPtr& segDir,
                                                       const IndexPartitionSchemaPtr& schema)
    : MergeStrategy(segDir, schema)
{
}

PriorityQueueMergeStrategy::~PriorityQueueMergeStrategy() {}

void PriorityQueueMergeStrategy::SetParameter(const MergeStrategyParameter& param)
{
    if (!mInputLimits.FromString(param.inputLimitParam)) {
        INDEXLIB_THROW(util::BadParameterException, "Invalid input limit parameter for merge strategy: [%s]",
                       +param.inputLimitParam.c_str());
    }

    if (!mOutputLimits.FromString(param.outputLimitParam)) {
        INDEXLIB_THROW(util::BadParameterException, "Invalid output limit parameter for merge strategy: [%s]",
                       +param.outputLimitParam.c_str());
    }

    if (!DecodeStrategyConditions(param.strategyConditions, mPriorityDesc, mMergeTriggerDesc)) {
        INDEXLIB_THROW(util::BadParameterException, "Invalid merge conditions for merge strategy: [%s]",
                       +param.strategyConditions.c_str());
    }
}

bool PriorityQueueMergeStrategy::DecodeStrategyConditions(const string& conditions, PriorityDescription& priorityDesc,
                                                          MergeTriggerDescription& mergeTriggerDesc)
{
    vector<string> mergeConditions;
    StringUtil::fromString(conditions, mergeConditions, ";");
    for (size_t i = 0; i < mergeConditions.size(); i++) {
        if (priorityDesc.FromString(mergeConditions[i])) {
            continue;
        }

        if (mergeTriggerDesc.FromString(mergeConditions[i])) {
            continue;
        }
        return false;
    }
    return true;
}

std::string PriorityQueueMergeStrategy::GetParameter() const
{
    stringstream ss;
    ss << "inputLimits[" << mInputLimits.ToString() << "],"
       << "mergeTriggerDesc[" << mMergeTriggerDesc.ToString() << "],"
       << "priorityDesc[" << mPriorityDesc.ToString() << "],"
       << "outputLimit[" << mOutputLimits.ToString() << "]";
    return ss.str();
}

MergeTask PriorityQueueMergeStrategy::CreateMergeTask(const SegmentMergeInfos& segMergeInfos,
                                                      const indexlibv2::framework::LevelInfo& levelInfo)
{
    indexlibv2::framework::LevelTopology topology = levelInfo.GetTopology();
    if (topology != indexlibv2::framework::topo_sequence) {
        INDEXLIB_FATAL_ERROR(UnSupported, " [%s] is unsupported, only support sequence",
                             indexlibv2::framework::LevelMeta::TopologyToStr(topology).c_str());
    }

    IE_LOG(INFO, "MergeParams:%s", GetParameter().c_str());
    PriorityQueue priorityQueue;
    PutSegMergeInfosToQueue(segMergeInfos, priorityQueue);

    // deletePercent & conflict segment num
    SegmentMergeInfos needMergeSegments;
    GenerateMergeSegments(priorityQueue, needMergeSegments);

    return GenerateMergeTask(needMergeSegments);
}

MergeTask PriorityQueueMergeStrategy::GenerateMergeTask(const SegmentMergeInfos& mergeInfos)
{
    SegmentMergeInfos needMergeSegments(mergeInfos.begin(), mergeInfos.end());
    MergeTask task;
    uint64_t totalMergeDocCount = 0;
    uint64_t totalMergedSize = 0;
    while (true) {
        SegmentMergeInfos inPlanSegments;
        ExtractMergeSegmentsForOnePlan(needMergeSegments, inPlanSegments, totalMergeDocCount, totalMergedSize);

        if (inPlanSegments.empty()) {
            break;
        }

        if (!IsPlanValid(inPlanSegments)) {
            IE_LOG(INFO,
                   "MergePlan has only one segment[%d] and "
                   "not LargerThanDelPercent!",
                   inPlanSegments[0].segmentId);
            continue;
        }

        MergePlan plan = ConstructMergePlan(inPlanSegments);
        task.AddPlan(plan);
        UpdateMergedInfo(inPlanSegments, totalMergeDocCount, totalMergedSize);
        IE_LOG(INFO, "MergePlan is [%s]", plan.ToString().c_str());
    }
    return task;
}

void PriorityQueueMergeStrategy::ExtractMergeSegmentsForOnePlan(SegmentMergeInfos& needMergeSegments,
                                                                SegmentMergeInfos& inPlanSegments,
                                                                uint64_t curTotalMergeDocCount,
                                                                uint64_t curTotalMergedSize)
{
    assert(inPlanSegments.empty());
    PriorityQueue queue;
    PutSegMergeInfosToQueue(needMergeSegments, queue);

    SegmentMergeInfos skipSegments;
    uint64_t inPlanMergeDocCount = 0;
    uint64_t inPlanMergedSize = 0;
    while (!queue.empty()) {
        QueueItem item = queue.top();
        queue.pop();

        uint32_t mergeDocCount = GetMergedDocCount(item.mergeInfo);
        uint64_t validSegmentSize = GetValidSegmentSize(item.mergeInfo);
        if (NeedSkipCurrentSegmentByDocCount(curTotalMergeDocCount, inPlanMergeDocCount, mergeDocCount) ||
            NeedSkipCurrentSegmentBySize(curTotalMergedSize, inPlanMergedSize, validSegmentSize)) {
            skipSegments.push_back(item.mergeInfo);
            continue;
        }

        inPlanSegments.push_back(item.mergeInfo);
        inPlanMergeDocCount += mergeDocCount;
        inPlanMergedSize += validSegmentSize;
    }
    needMergeSegments.swap(skipSegments);
}

uint32_t PriorityQueueMergeStrategy::GetMergedDocCount(const SegmentMergeInfo& info)
{
    return info.segmentInfo.docCount - info.deletedDocCount;
}

uint64_t PriorityQueueMergeStrategy::GetValidSegmentSize(const SegmentMergeInfo& info)
{
    uint32_t docCount = info.segmentInfo.docCount;
    if (docCount) {
        return info.segmentSize * (static_cast<double>(GetMergedDocCount(info)) / docCount);
    }
    return 0;
}

void PriorityQueueMergeStrategy::UpdateMergedInfo(const SegmentMergeInfos& inPlanSegments, uint64_t& mergedDocCount,
                                                  uint64_t& mergedSize)
{
    for (size_t i = 0; i < inPlanSegments.size(); ++i) {
        mergedDocCount += GetMergedDocCount(inPlanSegments[i]);
        mergedSize += GetValidSegmentSize(inPlanSegments[i]);
    }
}

bool PriorityQueueMergeStrategy::NeedSkipCurrentSegmentByDocCount(uint64_t curTotalMergeDocCount,
                                                                  uint64_t inPlanMergeDocCount,
                                                                  uint32_t curSegmentValidDocCount)
{
    if (inPlanMergeDocCount + curSegmentValidDocCount > mOutputLimits.maxMergedSegmentDocCount) {
        return true;
    }

    if (curTotalMergeDocCount + inPlanMergeDocCount + curSegmentValidDocCount > mOutputLimits.maxTotalMergedDocCount) {
        // skip for over total merge doc count
        return true;
    }

    return false;
}

bool PriorityQueueMergeStrategy::NeedSkipCurrentSegmentBySize(uint64_t curTotalMergedSize, uint64_t inPlanMergedSize,
                                                              uint64_t curSegmentValidSize)
{
    if (inPlanMergedSize + curSegmentValidSize > mOutputLimits.maxMergedSegmentSize) {
        return true;
    }

    if (curTotalMergedSize + inPlanMergedSize + curSegmentValidSize > mOutputLimits.maxTotalMergedSize) {
        // skip for over total merged size
        return true;
    }

    return false;
}

bool PriorityQueueMergeStrategy::IsPlanValid(const SegmentMergeInfos& planSegments)
{
    if (planSegments.empty()) {
        return false;
    }
    return planSegments.size() > 1 || LargerThanDelPercent(planSegments[0]);
}

MergePlan PriorityQueueMergeStrategy::ConstructMergePlan(SegmentMergeInfos& mergeInfos)
{
    MergePlan plan;
    for (size_t i = 0; i < mergeInfos.size(); i++) {
        plan.AddSegment(mergeInfos[i]);
    }
    return plan;
}

bool PriorityQueueMergeStrategy::LargerThanDelPercent(const SegmentMergeInfo& segMergeInfo)
{
    if (0 == segMergeInfo.segmentInfo.docCount) {
        return false;
    }
    uint32_t curPercent = ((double)segMergeInfo.deletedDocCount / segMergeInfo.segmentInfo.docCount) * 100;
    return curPercent > mMergeTriggerDesc.conflictDeletePercent;
}

void PriorityQueueMergeStrategy::GenerateMergeSegments(PriorityQueue& queue, SegmentMergeInfos& needMergeSegments)
{
    uint64_t curTotalMergeSize = 0;
    while (queue.size() >= mMergeTriggerDesc.conflictSegmentCount) {
        for (size_t i = 0; i < mMergeTriggerDesc.conflictSegmentCount; i++) {
            QueueItem item = queue.top();
            queue.pop();
            if (item.mergeInfo.segmentSize + curTotalMergeSize <= mInputLimits.maxTotalMergeSize) {
                needMergeSegments.push_back(item.mergeInfo);
                curTotalMergeSize += item.mergeInfo.segmentSize;
            }
        }
    }

    while (!queue.empty()) {
        QueueItem item = queue.top();
        queue.pop();
        if (LargerThanDelPercent(item.mergeInfo)) {
            if (item.mergeInfo.segmentSize + curTotalMergeSize <= mInputLimits.maxTotalMergeSize) {
                needMergeSegments.push_back(item.mergeInfo);
                curTotalMergeSize += item.mergeInfo.segmentSize;
            }
        }
    }

    stringstream ss;
    for (size_t i = 0; i < needMergeSegments.size(); i++) {
        ss << needMergeSegments[i].segmentId;
        if (i != needMergeSegments.size() - 1) {
            ss << ",";
        }
    }
    IE_LOG(INFO, "candidate merge segments:[%s]", ss.str().c_str());
}

void PriorityQueueMergeStrategy::PutSegMergeInfosToQueue(const SegmentMergeInfos& segMergeInfos,
                                                         PriorityQueue& priorityQueue)
{
    for (size_t i = 0; i < segMergeInfos.size(); i++) {
        uint32_t docCount = segMergeInfos[i].segmentInfo.docCount;
        if (docCount > mInputLimits.maxDocCount) {
            IE_LOG(INFO,
                   "merge ignore segment_[%d]"
                   " for over max-doc-count[%u], docCount[%u]",
                   segMergeInfos[i].segmentId, mInputLimits.maxDocCount, docCount);
            continue;
        }

        uint64_t segmentSize = segMergeInfos[i].segmentSize;
        if (segmentSize > mInputLimits.maxSegmentSize) {
            IE_LOG(INFO,
                   "merge ignore segment_[%d]"
                   " for over max-segment-size[%lu], segment size[%lu]",
                   segMergeInfos[i].segmentId, mInputLimits.maxSegmentSize, segmentSize);
            continue;
        }

        uint32_t validDocCount = docCount - segMergeInfos[i].deletedDocCount;
        if (validDocCount > mInputLimits.maxValidDocCount) {
            IE_LOG(INFO,
                   "merge ignore segment_[%d]"
                   " for over max-valid-doc-count[%u], validDocCount[%u]",
                   segMergeInfos[i].segmentId, mInputLimits.maxValidDocCount, validDocCount);
            continue;
        }

        uint64_t validSegmentSize = GetValidSegmentSize(segMergeInfos[i]);
        if (validSegmentSize > mInputLimits.maxValidSegmentSize) {
            IE_LOG(INFO,
                   "merge ignore segment_[%d]"
                   " for over max-valid-segment-size[%lu], valid segment size[%lu]",
                   segMergeInfos[i].segmentId, mInputLimits.maxValidSegmentSize, validSegmentSize);
            continue;
        }

        // check output limits
        if (validDocCount > mOutputLimits.maxMergedSegmentDocCount) {
            IE_LOG(INFO,
                   "merge ignore segment_[%d]"
                   " for over max-merged-segment-doc-count[%u], validDocCount[%u]",
                   segMergeInfos[i].segmentId, mOutputLimits.maxMergedSegmentDocCount, validDocCount);
            continue;
        }

        if (validDocCount > mOutputLimits.maxTotalMergedDocCount) {
            IE_LOG(INFO,
                   "merge ignore segment_[%d]"
                   " for over max-total-merge-doc-count[%u], validDocCount[%u]",
                   segMergeInfos[i].segmentId, mOutputLimits.maxTotalMergedDocCount, validDocCount);
            continue;
        }

        if (validSegmentSize > mOutputLimits.maxMergedSegmentSize) {
            IE_LOG(INFO,
                   "merge ignore segment_[%d]"
                   " for over max-merged-segment-size[%lu], validSegmentSize[%lu]",
                   segMergeInfos[i].segmentId, mOutputLimits.maxMergedSegmentSize, validSegmentSize);
            continue;
        }

        if (validSegmentSize > mOutputLimits.maxTotalMergedSize) {
            IE_LOG(INFO,
                   "merge ignore segment_[%d]"
                   " for over max-total-merge-doc-count[%lu], validDocCount[%lu]",
                   segMergeInfos[i].segmentId, mOutputLimits.maxTotalMergedSize, validSegmentSize);
            continue;
        }
        priorityQueue.push(MakeQueueItem(segMergeInfos[i]));
    }
}

QueueItem PriorityQueueMergeStrategy::MakeQueueItem(const SegmentMergeInfo& mergeInfo)
{
    QueueItem item;
    item.mergeInfo = mergeInfo;
    item.isAsc = mPriorityDesc.isAsc;
    switch (mPriorityDesc.type) {
    case FE_DOC_COUNT:
        item.feature = mergeInfo.segmentInfo.docCount;
        break;
    case FE_DELETE_DOC_COUNT:
        item.feature = mergeInfo.deletedDocCount;
        break;
    case FE_VALID_DOC_COUNT:
        item.feature = mergeInfo.segmentInfo.docCount - mergeInfo.deletedDocCount;
        break;
    case FE_TIMESTAMP:
        item.feature = mergeInfo.segmentInfo.timestamp;
        break;
    default:
        assert(false);
        break;
    }
    return item;
}

MergeTask PriorityQueueMergeStrategy::CreateMergeTaskForOptimize(const SegmentMergeInfos& segMergeInfos,
                                                                 const indexlibv2::framework::LevelInfo& levelInfo)
{
    OptimizeMergeStrategy strategy(mSegDir, mSchema);
    return strategy.CreateMergeTask(segMergeInfos, levelInfo);
}
}} // namespace indexlib::merger
