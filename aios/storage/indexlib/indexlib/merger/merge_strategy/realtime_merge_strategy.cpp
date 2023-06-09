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
#include "indexlib/merger/merge_strategy/realtime_merge_strategy.h"

#include <sstream>

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/merger/merge_strategy/optimize_merge_strategy.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, RealtimeMergeStrategy);

const string RealtimeMergeStrategy::MAX_SMALL_SEGMENT_COUNT = "max-small-segment-count";
const string RealtimeMergeStrategy::DO_MERGE_SIZE_THRESHOLD = "merge-size-upperbound";
const string RealtimeMergeStrategy::DONT_MERGE_SIZE_THRESHOLD = "merge-size-lowerbound";

RealtimeMergeStrategy::RealtimeMergeStrategy(const merger::SegmentDirectoryPtr& segDir,
                                             const IndexPartitionSchemaPtr& schema)
    : MergeStrategy(segDir, schema)
    , mMaxSmallSegmentCount(0)
    , mDoMergeSizeThreshold(0)
    , mDontMergeSizeThreshold(0)
{
}

RealtimeMergeStrategy::~RealtimeMergeStrategy() {}

void RealtimeMergeStrategy::SetParameter(const MergeStrategyParameter& param)
{
    string mergeParam = param.GetLegacyString();
    StringTokenizer st(mergeParam, ";", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    if (st.getNumTokens() != 3) {
        INDEXLIB_THROW(util::BadParameterException, "Invalid parameter for merge strategy: [%s]", mergeParam.c_str());
    }
    for (uint32_t i = 0; i < st.getNumTokens(); i++) {
        StringTokenizer st2(st[i], "=", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
        if (st2.getNumTokens() != 2) {
            INDEXLIB_THROW(util::BadParameterException, "Invalid parameter for merge strategy: [%s]", st[i].c_str());
        }

        if (st2[0] == MAX_SMALL_SEGMENT_COUNT) {
            int32_t maxSmallSegmentCount;
            if (!StringUtil::strToInt32(st2[1].c_str(), maxSmallSegmentCount) || maxSmallSegmentCount < 0) {
                INDEXLIB_THROW(util::BadParameterException, "Invalid parameter for merge strategy: [%s]",
                               st2[1].c_str());
            }
            mMaxSmallSegmentCount = maxSmallSegmentCount;
        } else if (st2[0] == DO_MERGE_SIZE_THRESHOLD) {
            int64_t doMergeSizeThreshold;
            if (!StringUtil::strToInt64(st2[1].c_str(), doMergeSizeThreshold) || doMergeSizeThreshold < 0) {
                INDEXLIB_THROW(util::BadParameterException, "Invalid parameter for merge strategy: [%s]",
                               st2[1].c_str());
            }
            mDoMergeSizeThreshold = (doMergeSizeThreshold << 20);
        } else if (st2[0] == DONT_MERGE_SIZE_THRESHOLD) {
            int64_t dontMergeSizeThreshold;
            if (!StringUtil::strToInt64(st2[1].c_str(), dontMergeSizeThreshold) || dontMergeSizeThreshold <= 0) {
                INDEXLIB_THROW(util::BadParameterException, "Invalid parameter for merge strategy: [%s]",
                               st2[1].c_str());
            }
            mDontMergeSizeThreshold = (dontMergeSizeThreshold << 20);
        } else {
            INDEXLIB_THROW(util::BadParameterException, "Invalid parameter for merge strategy: [%s]", st2[1].c_str());
        }
    }

    if (mDoMergeSizeThreshold < mDontMergeSizeThreshold) {
        INDEXLIB_THROW(util::BadParameterException, "%s[%ld] should not be less than %s[%ld]",
                       DO_MERGE_SIZE_THRESHOLD.c_str(), mDoMergeSizeThreshold, DONT_MERGE_SIZE_THRESHOLD.c_str(),
                       mDontMergeSizeThreshold);
    }
}

string RealtimeMergeStrategy::GetParameter() const
{
    stringstream ss;
    ss << MAX_SMALL_SEGMENT_COUNT << "=" << mMaxSmallSegmentCount << ";" << DO_MERGE_SIZE_THRESHOLD << "="
       << (mDoMergeSizeThreshold >> 20) << ";" << DONT_MERGE_SIZE_THRESHOLD << "=" << (mDontMergeSizeThreshold >> 20);
    return ss.str();
}

MergeTask RealtimeMergeStrategy::CreateMergeTask(const SegmentMergeInfos& segMergeInfos,
                                                 const indexlibv2::framework::LevelInfo& levelInfo)
{
    indexlibv2::framework::LevelTopology topology = levelInfo.GetTopology();
    if (topology != indexlibv2::framework::topo_sequence) {
        INDEXLIB_FATAL_ERROR(UnSupported, " [%s] is unsupported, only support sequence",
                             indexlibv2::framework::LevelMeta::TopologyToStr(topology).c_str());
    }

    MergeTask task;
    assert(segMergeInfos.size() > 0);

    int64_t totalSize = 0;
    int32_t segMergeInfoCount = (int32_t)segMergeInfos.size();
    int32_t segMergeInfoIdx, startMergeSegIdx = -1;
    bool needMerge = false;

    for (segMergeInfoIdx = segMergeInfoCount - 1; segMergeInfoIdx >= 0; segMergeInfoIdx--) {
        const SegmentMergeInfo& segMergeInfo = segMergeInfos[segMergeInfoIdx];
        if (segMergeInfo.segmentSize >= mDontMergeSizeThreshold) {
            startMergeSegIdx = segMergeInfoIdx + 1;
            if (startMergeSegIdx + mMaxSmallSegmentCount <= segMergeInfoCount) {
                needMerge = true;
            }
            break;
        }

        totalSize += segMergeInfo.segmentSize;
        if (totalSize >= mDoMergeSizeThreshold) {
            startMergeSegIdx = segMergeInfoIdx;
            needMerge = true;
            break;
        }
    }

    if (segMergeInfoIdx < 0 && segMergeInfoCount >= mMaxSmallSegmentCount) {
        needMerge = true;
        startMergeSegIdx = 0;
    }

    if (needMerge) {
        MergePlan plan;
        for (int32_t i = startMergeSegIdx; i <= segMergeInfoCount - 1; i++) {
            plan.AddSegment(segMergeInfos[i]);
        }
        task.AddPlan(plan);
    }

    return task;
}

MergeTask RealtimeMergeStrategy::CreateMergeTaskForOptimize(const SegmentMergeInfos& segMergeInfos,
                                                            const indexlibv2::framework::LevelInfo& levelInfo)
{
    OptimizeMergeStrategy strategy(mSegDir, mSchema);
    return strategy.CreateMergeTask(segMergeInfos, levelInfo);
}
}} // namespace indexlib::merger
