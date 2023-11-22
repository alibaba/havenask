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

#include <limits>
#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/SegmentGroupMetrics.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/segment_metrics_updater/max_min_segment_metrics_updater.h"
#include "indexlib/index/segment_metrics_updater/segment_metrics_updater.h"
#include "indexlib/index/segment_metrics_updater/time_series_segment_metrics_updater.h"
#include "indexlib/index/util/segment_directory_base.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/merge_plan.h"
#include "indexlib/merger/split_strategy/split_segment_strategy.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/KeyValueMap.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib { namespace merger {
class TimeSeriesSplitProcessor
{
public:
    TimeSeriesSplitProcessor() {}
    virtual ~TimeSeriesSplitProcessor() {}

    virtual bool Init(const std::string& attributeName, std::string& rangesStr,
                      const index_base::SegmentMergeInfos& segMergeInfos) = 0;

public:
    virtual segmentid_t GetDocSegment(const index::AttributeSegmentReaderWithCtx& segReader, docid_t localId) = 0;
};

template <typename T>
class TimeSeriesSplitProcessorImpl : public TimeSeriesSplitProcessor
{
public:
    bool Init(const std::string& attributeName, std::string& rangesStr,
              const index_base::SegmentMergeInfos& segMergeInfos) override
    {
        mAttributeName = attributeName;
        std::vector<std::string> rangesVec;
        autil::StringUtil::fromString(rangesStr, rangesVec, ",");
        for (const auto& range : rangesVec) {
            T value {};
            if (autil::StringUtil::fromString(range, value)) {
                mRanges.push_back(value);
            } else {
                INDEXLIB_FATAL_ERROR(BadParameter, "Invalid range [%s] for time series split strategy",
                                     rangesStr.c_str());
                return false;
            }
        }
        std::sort(mRanges.begin(), mRanges.end());
        for (const auto& segMergeInfo : segMergeInfos) {
            std::shared_ptr<framework::SegmentGroupMetrics> segmentGroupMetrics =
                segMergeInfo.segmentMetrics.GetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP);
            if (!segmentGroupMetrics) {
                IE_LOG(WARN, "cannot get segmentGroupMetrics for segment [%d]", segMergeInfo.segmentId);
                continue;
            }
            T baseline = std::numeric_limits<T>::max();
            if (!GetBaseline(segmentGroupMetrics, mAttributeName, baseline)) {
                IE_LOG(WARN, "segment [%d] cannot get baseline", segMergeInfo.segmentId);
            } else if (baseline > mAttributeBaseline) {
                mAttributeBaseline = baseline;
            }
        }
        return true;
    }
    TimeSeriesSplitProcessorImpl() : mAttributeBaseline(std::numeric_limits<T>::min()) {}

    ~TimeSeriesSplitProcessorImpl() {}
    segmentid_t GetDocSegment(const index::AttributeSegmentReaderWithCtx& segReader, docid_t localId) override
    {
        T value {};
        bool isNull = false;
        // TODO support
        if (segReader.reader == nullptr ||
            !segReader.reader->Read(localId, segReader.ctx, reinterpret_cast<uint8_t*>(&value), sizeof(value),
                                    isNull)) {
            INDEXLIB_FATAL_ERROR(Runtime, "read local docid [%d] failed", localId);
        }
        return GetSegmentIdx(value);
    }

private:
    std::string mAttributeName;
    std::vector<T> mRanges;
    T mAttributeBaseline;

private:
    bool GetBaseline(std::shared_ptr<framework::SegmentGroupMetrics> segmentGroupMetrics, const std::string& attrName,
                     T& baseline)
    {
        if (index::TimeSeriesSegmentMetricsUpdater::GetBaseline(segmentGroupMetrics, attrName, baseline)) {
            return true;
        }
        IE_LOG(WARN, "can not find baseline meta for [%s]", attrName.c_str());
        T tmp {};
        return index::MaxMinSegmentMetricsUpdater::GetAttrValues<T>(segmentGroupMetrics, attrName, baseline, tmp);
    }

    segmentid_t GetSegmentIdx(T value)
    {
        if (value > mAttributeBaseline) {
            IE_LOG(WARN, "value [%s] is greater than mAttributeBaseline [%s]",
                   autil::StringUtil::toString(value).c_str(), autil::StringUtil::toString(mAttributeBaseline).c_str());
            return 0;
        }
        value = mAttributeBaseline - value;
        for (size_t i = 0; i < mRanges.size(); ++i) {
            if (value < mRanges[i]) {
                return i;
            }
        }
        return mRanges.size();
    }

private:
    IE_LOG_DECLARE();
};

class TimeSeriesSplitStrategy : public SplitSegmentStrategy
{
public:
    TimeSeriesSplitStrategy(index::SegmentDirectoryBasePtr segDir, config::IndexPartitionSchemaPtr schema,
                            index::OfflineAttributeSegmentReaderContainerPtr attrReaders,
                            const index_base::SegmentMergeInfos& segMergeInfos, const MergePlan& plan,
                            std::map<segmentid_t, index::SegmentMetricsUpdaterPtr> hintDocInfos,
                            const util::MetricProviderPtr& metrics);
    ~TimeSeriesSplitStrategy();

public:
    bool Init(const util::KeyValueMap& parameters) override;
    segmentindex_t DoProcess(segmentid_t oldSegId, docid_t oldLocalId, int64_t& hintValue) override;

public:
    static const std::string STRATEGY_NAME;

private:
    typedef std::map<std::pair<std::string, segmentid_t>, index::AttributeSegmentReaderWithCtx> ReaderCache;
    ReaderCache mReaderCache;
    autil::mem_pool::Pool mPool;
    TimeSeriesSplitProcessor* mTimeSeriesSplitProcessor;
    std::string mAttributeName;
    FieldType mFieldType;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TimeSeriesSplitStrategy);
}} // namespace indexlib::merger
