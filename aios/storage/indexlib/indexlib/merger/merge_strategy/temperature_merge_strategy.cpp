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
#include "indexlib/merger/merge_strategy/temperature_merge_strategy.h"

#include "autil/StringUtil.h"
#include "autil/legacy/json.h"
#include "indexlib/index/segment_metrics_updater/temperature_segment_metrics_updater.h"
#include "indexlib/merger/merge_strategy/optimize_merge_strategy.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace indexlib::index;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, TemperatureMergeStrategy);

/*
  config example
  priority-feature 可选doc_count;delete-doc-count;temperature_conflict三种
  "merge_config":
  {
    "merge_strategy" : "temperature",
    "merge_strategy_params" : {
        "output_limits" : "max_hot_segment_size=1024;max_warm_segment_size=1024;max_cold_segment_size=10240",
        "strategy_conditions " : "priority-feature=doc-count#asc;select-sequence=HOT,WARM,COLD"
    }
  }

*/

const int64_t TemperatureMergeStrategy::DEFAULT_MAX_HOT_SEGMENT_SIZE = 10240;
const int64_t TemperatureMergeStrategy::DEFAULT_MAX_WARM_SEGMENT_SIZE = 15360;
const int64_t TemperatureMergeStrategy::DEFAULT_MAX_COLD_SEGMENT_SIZE = 20480;
const int64_t TemperatureMergeStrategy::SIZE_MB = 1048576;

TemperatureMergeStrategy::~TemperatureMergeStrategy() {}

void TemperatureMergeStrategy::SetParameter(const config::MergeStrategyParameter& param)
{
    string outputLimit = param.outputLimitParam;
    AnalyseOuputLimit(outputLimit);
    string strategeCondition = param.strategyConditions;
    AnalyseCondition(strategeCondition);
}

void TemperatureMergeStrategy::AnalyseCondition(const string& condition)
{
    vector<string> conditions = StringUtil::split(condition, ";");
    if (conditions.empty()) {
        INDEXLIB_FATAL_ERROR(BadParameter, "Invalid parameter for merge strategy: [%s]", condition.c_str());
    }

    if (!mPriority.FromString(conditions[0])) {
        INDEXLIB_FATAL_ERROR(BadParameter, "Invalid parameter for merge strategy: [%s]", condition.c_str());
    }

    if (mPriority.type != FE_DOC_TEMPERTURE_DIFFERENCE) {
        return;
    }

    vector<string> selectSeqs = StringUtil::split(conditions[1], "=");
    if (selectSeqs.size() != 2 && selectSeqs[0] != "select-sequence") {
        INDEXLIB_FATAL_ERROR(BadParameter, "Invalid parameter for merge strategy: [%s]", condition.c_str());
    }
    mSeq = StringUtil::split(selectSeqs[1], ",");
    if (mSeq.empty()) {
        INDEXLIB_FATAL_ERROR(BadParameter, "Invalid parameter for merge strategy: [%s]", condition.c_str());
    }
    for (const auto& temperature : mSeq) {
        if (temperature != "HOT" && temperature != "WARM" && temperature != "COLD") {
            INDEXLIB_FATAL_ERROR(BadParameter, "Invalid parameter for merge strategy: [%s]", condition.c_str());
        }
    }
    if (mSeq[0] == "COLD") {
        INDEXLIB_FATAL_ERROR(BadParameter, "Invalid parameter for merge strategy: [%s]", condition.c_str());
    }
}

void TemperatureMergeStrategy::AnalyseOuputLimit(const string& outputLimit)
{
    vector<string> limits = StringUtil::split(outputLimit, ";");
    for (auto& limit : limits) {
        vector<string> segmentSize = StringUtil::split(limit, "=");
        if (segmentSize.size() != 2) {
            INDEXLIB_FATAL_ERROR(BadParameter, "Invalid parameter for merge strategy: [%s]", limit.c_str());
        }
        StringUtil::trim(segmentSize[0]);
        StringUtil::trim(segmentSize[1]);
        if (segmentSize[0] == "max_hot_segment_size") {
            if (!StringUtil::fromString(segmentSize[1], mMaxHotSegmentSize)) {
                INDEXLIB_FATAL_ERROR(BadParameter, "Invalid parameter for merge strategy: [%s]", limit.c_str());
            }
        } else if (segmentSize[0] == "max_warm_segment_size") {
            if (!StringUtil::fromString(segmentSize[1], mMaxWarmSegmentSize)) {
                INDEXLIB_FATAL_ERROR(BadParameter, "Invalid parameter for merge strategy: [%s]", limit.c_str());
            }
        } else if (segmentSize[0] == "max_cold_segment_size") {
            if (!StringUtil::fromString(segmentSize[1], mMaxColdSegmentSize)) {
                INDEXLIB_FATAL_ERROR(BadParameter, "Invalid parameter for merge strategy: [%s]", limit.c_str());
            }
        }
    }
    mMaxColdSegmentSize *= SIZE_MB;
    mMaxWarmSegmentSize *= SIZE_MB;
    mMaxHotSegmentSize *= SIZE_MB;
}

string TemperatureMergeStrategy::GetParameter() const
{
    stringstream ss;
    ss << "max_hot_segment_size=" << mMaxHotSegmentSize << ";"
       << "max_warm_segment_size=" << mMaxWarmSegmentSize << ";"
       << "max_cold_segment_size=" << mMaxColdSegmentSize << ";"
       << "priorityDesc=" << mPriority.ToString() << ";"
       << "sequence=" << ToJsonString(mSeq) << ";";
    return ss.str();
}

MergeTask TemperatureMergeStrategy::CreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos,
                                                    const indexlibv2::framework::LevelInfo& levelInfo)
{
    indexlibv2::framework::LevelTopology topology = levelInfo.GetTopology();
    if (topology != indexlibv2::framework::topo_sequence) {
        INDEXLIB_FATAL_ERROR(UnSupported, " [%s] is unsupported, only support sequence",
                             indexlibv2::framework::LevelMeta::TopologyToStr(topology).c_str());
    }
    IE_LOG(INFO, "MergeParams:%s", GetParameter().c_str());
    vector<PriorityQueue> queues;
    if (mPriority.type == FE_DOC_TEMPERTURE_DIFFERENCE) {
        for (size_t i = 0; i < mSeq.size(); i++) {
            PriorityQueue priorityQueue;
            queues.push_back(priorityQueue);
        }
    } else {
        PriorityQueue priorityQueue;
        queues.push_back(priorityQueue);
    }
    PutSegMergeInfosToQueue(segMergeInfos, queues);
    return GenerateMergeTask(queues);
}

void TemperatureMergeStrategy::GetValidSegmentSize(const SegmentMergeInfo& info, int64_t& hotSegmentSize,
                                                   int64_t& warmSegmentSize, int64_t& coldSegmentSize)
{
    uint32_t docCount = info.segmentInfo.docCount;
    if (docCount == 0) {
        return;
    }
    auto groupMetrics = info.segmentMetrics.GetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP);
    if (!groupMetrics) {
        INDEXLIB_FATAL_ERROR(Runtime, "get custom mertrics failed");
    }

    json::JsonMap temperature = groupMetrics->Get<json::JsonMap>(TEMPERATURE_SEGMENT_METIRC_KRY);
    string lifeCycleDetail = AnyCast<string>(temperature[LIFECYCLE_DETAIL]);
    int64_t totalDocCount = 0, hotDocCount = 0, warmDocCount = 0, coldDocCount = 0;
    AnalyseLifeCycleDetail(lifeCycleDetail, totalDocCount, hotDocCount, warmDocCount, coldDocCount);
    hotSegmentSize = info.segmentSize * (static_cast<double>(hotDocCount) / docCount);
    warmSegmentSize = info.segmentSize * (static_cast<double>(warmDocCount) / docCount);
    coldSegmentSize = info.segmentSize * (static_cast<double>(coldDocCount) / docCount);
}

MergeTask TemperatureMergeStrategy::GenerateMergeTask(vector<PriorityQueue>& queues)
{
    int cursor = 0;
    SegmentMergeInfos needMergeSegments;
    int64_t totalHotSegmentSize = 0, totalWarmSegmentSize = 0, totalColdSegmentSize = 0;
    while (cursor < queues.size()) {
        while (!queues[cursor].empty()) {
            QueueItem item = queues[cursor].top();
            queues[cursor].pop();
            int64_t hotSegmentSize = 0, warmSegmentSize = 0, coldSegmentSize = 0;
            GetValidSegmentSize(item.mergeInfo, hotSegmentSize, warmSegmentSize, coldSegmentSize);
            if (totalHotSegmentSize + hotSegmentSize <= mMaxHotSegmentSize &&
                totalWarmSegmentSize + warmSegmentSize <= mMaxWarmSegmentSize &&
                totalColdSegmentSize + coldSegmentSize <= mMaxColdSegmentSize) {
                totalHotSegmentSize += hotSegmentSize;
                totalWarmSegmentSize += warmSegmentSize;
                totalColdSegmentSize += coldSegmentSize;
                needMergeSegments.push_back(item.mergeInfo);
            }
        }
        cursor++;
    }
    MergePlan plan;
    for (const auto& mergeInfo : needMergeSegments) {
        plan.AddSegment(mergeInfo);
    }
    MergeTask task;
    task.AddPlan(plan);
    IE_LOG(INFO, "MergePlan is [%s]", plan.ToString().c_str());
    return task;
}

TemperatureProperty TemperatureMergeStrategy::GetSegmentTemperature(const index_base::SegmentMergeInfo& mergeInfo)
{
    auto groupMetrics = mergeInfo.segmentMetrics.GetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP);
    if (!groupMetrics) {
        INDEXLIB_FATAL_ERROR(Runtime, "cannot get custom group metric for segment [%d]", mergeInfo.segmentId);
    }
    json::JsonMap temperature = groupMetrics->Get<json::JsonMap>(TEMPERATURE_SEGMENT_METIRC_KRY);
    return TemperatureSegmentMetricsUpdater::StringToTemperatureProperty(AnyCast<string>(temperature[LIFECYCLE]));
}

void TemperatureMergeStrategy::PutSegMergeInfosToQueue(const index_base::SegmentMergeInfos& segMergeInfos,
                                                       vector<PriorityQueue>& priorityQueues)
{
    for (size_t i = 0; i < segMergeInfos.size(); i++) {
        if (priorityQueues.size() == 1 && mPriority.type != FE_DOC_TEMPERTURE_DIFFERENCE) {
            priorityQueues[0].push(MakeQueueItem(segMergeInfos[i]));
        } else {
            TemperatureProperty property = GetSegmentTemperature(segMergeInfos[i]);
            int64_t seqLocation = -1;
            switch (property) {
            case TemperatureProperty::HOT:
                seqLocation = FindQueueIndex("HOT");
                break;
            case TemperatureProperty::WARM:
                seqLocation = FindQueueIndex("WARM");
                break;
            case TemperatureProperty::COLD:
                seqLocation = FindQueueIndex("COLD");
                break;
            default:
                assert(false);
            }
            if (seqLocation != -1) {
                priorityQueues[seqLocation].push(MakeQueueItem(segMergeInfos[i]));
            }
        }
    }
}

int32_t TemperatureMergeStrategy::FindQueueIndex(const std::string& temperature)
{
    for (size_t i = 0; i < mSeq.size(); i++) {
        if (mSeq[i] == temperature) {
            return i;
        }
    }
    return -1;
}

int64_t TemperatureMergeStrategy::CaculatorTemperatureDiff(const SegmentMergeInfo& mergeInfo)
{
    uint32_t docCount = mergeInfo.segmentInfo.docCount;
    if (docCount == 0) {
        return 0;
    }
    if (GetSegmentTemperature(mergeInfo) == TemperatureProperty::COLD) {
        return mergeInfo.segmentSize *
               (static_cast<double>(mergeInfo.segmentInfo.docCount - mergeInfo.deletedDocCount) / docCount);
    }
    auto groupMetrics = mergeInfo.segmentMetrics.GetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP);
    if (!groupMetrics) {
        INDEXLIB_FATAL_ERROR(Runtime, "cannot get custom group metric");
    }
    json::JsonMap temperature = groupMetrics->Get<json::JsonMap>(TEMPERATURE_SEGMENT_METIRC_KRY);
    string lifeCycleDetail = AnyCast<string>(temperature[LIFECYCLE_DETAIL]);
    int64_t totalDocCount = 0, hotDocCount = 0, warmDocCount = 0, coldDocCount = 0;
    AnalyseLifeCycleDetail(lifeCycleDetail, totalDocCount, hotDocCount, warmDocCount, coldDocCount);
    if (GetSegmentTemperature(mergeInfo) == TemperatureProperty::HOT) {
        return (warmDocCount * 1.0 / totalDocCount) * 100 + (coldDocCount * 1.0 / totalDocCount) * 200;
    } else {
        return (coldDocCount * 1.0 / totalDocCount) * 200;
    }
    return 0;
}

void TemperatureMergeStrategy::AnalyseLifeCycleDetail(const string& detail, int64_t& totalDocCount,
                                                      int64_t& hotDocCount, int64_t& warmDocCount,
                                                      int64_t& coldDocCount)
{
    vector<string> docDetails = StringUtil::split(detail, ";");
    if (docDetails.size() != 3) {
        INDEXLIB_FATAL_ERROR(Runtime, "lifecycle detail [%s] is invalid", detail.c_str());
    }
    for (size_t i = 0; i < docDetails.size(); i++) {
        auto docInfo = StringUtil::split(docDetails[i], ":");
        if (docInfo.size() != 2) {
            INDEXLIB_FATAL_ERROR(Runtime, "doc detail [%s] is invalid", docDetails[i].c_str());
        }
        if (i == 0) {
            if (!StringUtil::fromString(docInfo[1], hotDocCount)) {
                INDEXLIB_FATAL_ERROR(Runtime, "doc detail [%s] is invalid", docDetails[i].c_str());
            }
        } else if (i == 1) {
            if (!StringUtil::fromString(docInfo[1], warmDocCount)) {
                INDEXLIB_FATAL_ERROR(Runtime, "doc detail [%s] is invalid", docDetails[i].c_str());
            }
        } else {
            if (!StringUtil::fromString(docInfo[1], coldDocCount)) {
                INDEXLIB_FATAL_ERROR(Runtime, "doc detail [%s] is invalid", docDetails[i].c_str());
            }
        }
    }
    totalDocCount = hotDocCount + warmDocCount + coldDocCount;
}

QueueItem TemperatureMergeStrategy::MakeQueueItem(const SegmentMergeInfo& mergeInfo)
{
    QueueItem item;
    item.mergeInfo = mergeInfo;
    item.isAsc = mPriority.isAsc;
    switch (mPriority.type) {
    case FE_DOC_COUNT:
        item.feature = mergeInfo.segmentInfo.docCount;
        break;
    case FE_DELETE_DOC_COUNT:
        item.feature = mergeInfo.deletedDocCount;
        break;
    case FE_DOC_TEMPERTURE_DIFFERENCE:
        if (item.isAsc) {
            IE_LOG(WARN, "temperature unsupport asc, change to desc");
            item.isAsc = false;
        }
        item.feature = CaculatorTemperatureDiff(mergeInfo);
        break;
    default:
        IE_LOG(WARN, "temperature not support type [%d]", mPriority.type);
        break;
    }
    return item;
}

MergeTask TemperatureMergeStrategy::CreateMergeTaskForOptimize(const SegmentMergeInfos& segMergeInfos,
                                                               const indexlibv2::framework::LevelInfo& levelInfo)
{
    OptimizeMergeStrategy strategy(mSegDir, mSchema);
    return strategy.CreateMergeTask(segMergeInfos, levelInfo);
}
}} // namespace indexlib::merger
