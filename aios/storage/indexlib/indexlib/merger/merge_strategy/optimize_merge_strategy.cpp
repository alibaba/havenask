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
#include "indexlib/merger/merge_strategy/optimize_merge_strategy.h"

#include <algorithm>
#include <assert.h>
#include <limits>
#include <memory>
#include <ostream>
#include <stddef.h>

#include "alog/Logger.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/config/adaptive_dictionary_schema.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/load_config_list.h"
#include "indexlib/config/truncate_profile_schema.h"
#include "indexlib/file_system/load_config/LoadStrategy.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::common;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, OptimizeMergeStrategy);

const uint32_t OptimizeMergeStrategy::DEFAULT_MAX_DOC_COUNT = std::numeric_limits<uint32_t>::max();
const uint64_t OptimizeMergeStrategy::DEFAULT_AFTER_MERGE_MAX_DOC_COUNT = std::numeric_limits<uint64_t>::max();
const uint32_t OptimizeMergeStrategy::INVALID_AFTER_MERGE_MAX_SEGMENT_COUNT = std::numeric_limits<uint32_t>::max();

OptimizeMergeStrategy::OptimizeMergeStrategy(const merger::SegmentDirectoryPtr& segDir,
                                             const IndexPartitionSchemaPtr& schema)
    : MergeStrategy(segDir, schema)
    , mMaxDocCount(DEFAULT_MAX_DOC_COUNT)
    , mAfterMergeMaxDocCount(DEFAULT_AFTER_MERGE_MAX_DOC_COUNT)
    , mAfterMergeMaxSegmentCount(INVALID_AFTER_MERGE_MAX_SEGMENT_COUNT)
    , mSkipSingleMergedSegment(true)
{
}

OptimizeMergeStrategy::~OptimizeMergeStrategy() {}

void OptimizeMergeStrategy::SetParameter(const MergeStrategyParameter& param)
{
    string mergeParam = param.GetLegacyString();
    StringUtil::trim(mergeParam);
    if (mergeParam.empty()) {
        return;
    }

    StringTokenizer params(mergeParam, ";", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (size_t i = 0; i < params.getNumTokens(); i++) {
        StringTokenizer kvStr(params[i], "=", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
        if (kvStr.getNumTokens() != 2) {
            INDEXLIB_FATAL_ERROR(BadParameter, "Invalid parameter for merge strategy: [%s]", mergeParam.c_str());
        }
        if (kvStr[0] == "max-doc-count") {
            uint32_t maxDocCount;
            if (!StringUtil::strToUInt32(kvStr[1].c_str(), maxDocCount)) {
                INDEXLIB_FATAL_ERROR(BadParameter, "Invalid parameter for merge strategy: [%s]", params[i].c_str());
            }
            mMaxDocCount = maxDocCount;
        } else if (kvStr[0] == "after-merge-max-doc-count") {
            uint32_t afterMergeMaxDocCount;
            if (!StringUtil::strToUInt32(kvStr[1].c_str(), afterMergeMaxDocCount)) {
                INDEXLIB_FATAL_ERROR(BadParameter, "Invalid parameter for merge strategy: [%s]", params[i].c_str());
            }
            mAfterMergeMaxDocCount = afterMergeMaxDocCount;
        } else if (kvStr[0] == "after-merge-max-segment-count") {
            uint32_t afterMergeMaxSegmentCount;
            if (!StringUtil::strToUInt32(kvStr[1].c_str(), afterMergeMaxSegmentCount)) {
                INDEXLIB_FATAL_ERROR(BadParameter, "Invalid parameter for merge strategy: [%s]", params[i].c_str());
            }
            mAfterMergeMaxSegmentCount = afterMergeMaxSegmentCount;
        } else if (kvStr[0] == "skip-single-merged-segment") {
            mSkipSingleMergedSegment = (kvStr[1] != "false");
        } else {
            INDEXLIB_FATAL_ERROR(BadParameter, "Invalid parameter for merge strategy: [%s]", params[i].c_str());
        }
    }
    if (mAfterMergeMaxSegmentCount != INVALID_AFTER_MERGE_MAX_SEGMENT_COUNT &&
        mAfterMergeMaxDocCount != DEFAULT_AFTER_MERGE_MAX_DOC_COUNT) {
        INDEXLIB_FATAL_ERROR(BadParameter, "Invalid parameter for merge strategy: [%s]", mergeParam.c_str());
    }

    if (IsMergeToMultiSegments() && mSchema) {
        if (mSchema->GetAdaptiveDictSchema() || mSchema->GetTruncateProfileSchema()) {
            INDEXLIB_FATAL_ERROR(BadParameter, "optimize merge to multi segments"
                                               " does not support truncate or adaptive bitmap");
        }
    }
}

string OptimizeMergeStrategy::GetParameter() const
{
    if (mMaxDocCount == DEFAULT_MAX_DOC_COUNT && mAfterMergeMaxDocCount == DEFAULT_AFTER_MERGE_MAX_DOC_COUNT) {
        return "";
    }
    stringstream ss;
    ss << "max-doc-count=" << mMaxDocCount << ";"
       << "after-merge-max-doc-count" << mAfterMergeMaxDocCount;
    return ss.str();
}

MergeTask OptimizeMergeStrategy::CreateMergeTask(const SegmentMergeInfos& segMergeInfos,
                                                 const indexlibv2::framework::LevelInfo& levelInfo)
{
    indexlibv2::framework::LevelTopology topology = levelInfo.GetTopology();
    if (topology != indexlibv2::framework::topo_sequence) {
        INDEXLIB_FATAL_ERROR(UnSupported, " [%s] is unsupported, only support sequence",
                             indexlibv2::framework::LevelMeta::TopologyToStr(topology).c_str());
    }

    SegmentMergeInfos tmpMergeInfos = FilterSegment(mMaxDocCount, segMergeInfos);
    return DoCreateMergeTask(tmpMergeInfos);
}

bool OptimizeMergeStrategy::IsMergeToMultiSegments()
{
    return (mAfterMergeMaxDocCount != DEFAULT_AFTER_MERGE_MAX_DOC_COUNT ||
            mAfterMergeMaxSegmentCount != INVALID_AFTER_MERGE_MAX_SEGMENT_COUNT);
}

uint64_t OptimizeMergeStrategy::SplitDocCount(const SegmentMergeInfos& segMergeInfos) const
{
    uint64_t totalDocCount = 0;
    for (size_t i = 0; i < segMergeInfos.size(); i++) {
        totalDocCount += segMergeInfos[i].segmentInfo.docCount - segMergeInfos[i].deletedDocCount;
    }
    return totalDocCount / mAfterMergeMaxSegmentCount + 1;
}

MergeTask OptimizeMergeStrategy::DoCreateMergeTask(const SegmentMergeInfos& segMergeInfos)
{
    MergeTask task;
    vector<MergePlan> plans;
    if (mAfterMergeMaxSegmentCount != INVALID_AFTER_MERGE_MAX_SEGMENT_COUNT) {
        mAfterMergeMaxDocCount = SplitDocCount(segMergeInfos);
    }
    size_t segmentInfoId = 0;
    while (segmentInfoId < segMergeInfos.size()) {
        MergePlan plan;
        uint64_t docCount = 0;
        do {
            const SegmentMergeInfo& segmentInfo = segMergeInfos[segmentInfoId];
            uint32_t acutalSegmentDocCount = segmentInfo.segmentInfo.docCount - segmentInfo.deletedDocCount;
            plan.AddSegment(segmentInfo);
            docCount += acutalSegmentDocCount;
            segmentInfoId++;
        } while (segmentInfoId < segMergeInfos.size() && docCount < mAfterMergeMaxDocCount);
        plans.push_back(plan);
    }

    if (!NeedMerge(plans, segMergeInfos)) {
        return task;
    }
    for (size_t i = 0; i < plans.size(); i++) {
        IE_LOG(INFO, "add optimize merge plan [%s]", plans[i].ToString().c_str());
        task.AddPlan(plans[i]);
    }

    return task;
}

SegmentMergeInfos OptimizeMergeStrategy::FilterSegment(uint32_t maxDocCountInSegment,
                                                       const SegmentMergeInfos& segMergeInfos)
{
    SegmentMergeInfos filterMergeInfos;
    if (maxDocCountInSegment == DEFAULT_MAX_DOC_COUNT) {
        return segMergeInfos;
    }
    SegmentMergeInfos::const_iterator it = segMergeInfos.begin();
    for (; it != segMergeInfos.end(); ++it) {
        if (it->segmentInfo.docCount <= maxDocCountInSegment) {
            filterMergeInfos.push_back(*it);
        }
    }
    return filterMergeInfos;
}

MergeTask OptimizeMergeStrategy::CreateMergeTaskForOptimize(const SegmentMergeInfos& segMergeInfos,
                                                            const indexlibv2::framework::LevelInfo& levelInfo)
{
    indexlibv2::framework::LevelTopology topology = levelInfo.GetTopology();
    if (topology != indexlibv2::framework::topo_sequence) {
        INDEXLIB_FATAL_ERROR(UnSupported, " [%s] is unsupported, only support sequence",
                             indexlibv2::framework::LevelMeta::TopologyToStr(topology).c_str());
    }
    return DoCreateMergeTask(segMergeInfos);
}

bool OptimizeMergeStrategy::NeedMerge(const vector<MergePlan>& plans, const SegmentMergeInfos& segMergeInfos)
{
    for (size_t i = 0; i < plans.size(); ++i) {
        if (plans[i].GetSegmentCount() > 1) {
            return true;
        }
        assert(plans[i].GetSegmentCount() == 1);
        segmentid_t segmentId = plans[i].CreateIterator().Next();
        if (!mSkipSingleMergedSegment || !IsMergedSegment(segmentId, segMergeInfos)) {
            return true;
        }
    }
    return false;
}

bool OptimizeMergeStrategy::IsMergedSegment(segmentid_t segmentId, const SegmentMergeInfos& segMergeInfos)
{
    for (size_t i = 0; i < segMergeInfos.size(); ++i) {
        if (segmentId == segMergeInfos[i].segmentId) {
            return segMergeInfos[i].segmentInfo.mergedSegment;
        }
    }
    assert(false);
    return false;
}
}} // namespace indexlib::merger
