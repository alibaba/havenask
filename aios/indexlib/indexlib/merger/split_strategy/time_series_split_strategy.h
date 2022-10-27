#ifndef __INDEXLIB_TIME_SERIES_SPLIT_STRATEGY_H
#define __INDEXLIB_TIME_SERIES_SPLIT_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/merger/split_strategy/split_segment_strategy.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/util/key_value_map.h"
#include "indexlib/index/time_series_segment_metrics_updater.h"
#include "indexlib/index/max_min_segment_metrics_updater.h"

IE_NAMESPACE_BEGIN(merger);
class TimeSeriesSplitProcessor
{
public:
    TimeSeriesSplitProcessor() {}
    virtual ~TimeSeriesSplitProcessor() {}

    virtual bool Init(const std::string& attributeName, std::string& rangesStr,
        const index_base::SegmentMergeInfos& segMergeInfos)
        = 0;

public:
    virtual segmentid_t GetDocSegment(
            const index::AttributeSegmentReaderPtr& segReader, docid_t localId) = 0;
};

template <typename T> 
class TimeSeriesSplitProcessorImpl : public TimeSeriesSplitProcessor
{
public:
    bool Init(const std::string& attributeName,
              std::string& rangesStr, const index_base::SegmentMergeInfos& segMergeInfos) override
    {
        mAttributeName = attributeName;
        std::vector<std::string> rangesVec;
        autil::StringUtil::fromString(rangesStr, rangesVec, ",");
        for (const auto& range : rangesVec)
        {
            T value;
            if (autil::StringUtil::fromString(range, value))
            {
                mRanges.push_back(value);
            }
            else
            {
                INDEXLIB_FATAL_ERROR(  
                        BadParameter, "Invalid range [%s] for time series split strategy", 
                        rangesStr.c_str()); 
                return false;
            }
        }
        std::sort(mRanges.begin(), mRanges.end());
        for (const auto& segMergeInfo : segMergeInfos)
        {
            index_base::SegmentGroupMetricsPtr segmentGroupMetrics
                = segMergeInfo.segmentMetrics.GetSegmentCustomizeGroupMetrics();
            if (!segmentGroupMetrics)
            {
                IE_LOG(WARN, "cannot get segmentGroupMetrics for segment [%d]",
                    segMergeInfo.segmentId);
                continue;
            }
            T baseline = std::numeric_limits<T>::max();
            if (!GetBaseline(segmentGroupMetrics, mAttributeName, baseline))
            {
                IE_LOG(WARN, "segment [%d] cannot get baseline", segMergeInfo.segmentId);
            }
            else if (baseline > mAttributeBaseline)
            {
                mAttributeBaseline = baseline;
            }
        }
        return true;
    }
    TimeSeriesSplitProcessorImpl()
        : mAttributeBaseline(std::numeric_limits<T>::min())
    {
    }

    ~TimeSeriesSplitProcessorImpl() {}
    segmentid_t GetDocSegment(
        const index::AttributeSegmentReaderPtr& segReader, docid_t localId) override
    {
        T value;
        if (segReader == nullptr || !segReader->Read(localId, reinterpret_cast<uint8_t*>(&value), sizeof(value)))
        {
            INDEXLIB_FATAL_ERROR(Runtime, "read local docid [%d] failed", localId);
        }
        return GetSegmentIdx(value);
    }

private:
    std::string mAttributeName;
    std::vector<T> mRanges;
    T mAttributeBaseline;

private:
    bool GetBaseline(index_base::SegmentGroupMetricsPtr segmentGroupMetrics,
        const std::string& attrName, T& baseline)
    {
        if (index::TimeSeriesSegmentMetricsUpdater::GetBaseline(
                segmentGroupMetrics, attrName, baseline))
        {
            return true;
        }
        IE_LOG(WARN, "can not find baseline meta for [%s]", attrName.c_str());
        T tmp {};
        return index::MaxMinSegmentMetricsUpdater::GetAttrValues<T>(
            segmentGroupMetrics, attrName, baseline, tmp);
    }

    segmentid_t GetSegmentIdx(T value) 
    {
        if (value > mAttributeBaseline)
        {
            IE_LOG(WARN, "value [%s] is greater than mAttributeBaseline [%s]", autil::StringUtil::toString(value).c_str(), autil::StringUtil::toString(mAttributeBaseline).c_str());
            return 0;
        }
        value = mAttributeBaseline - value;
        for (size_t i = 0; i < mRanges.size(); ++i)
        {
            if (value < mRanges[i])
            {
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
    TimeSeriesSplitStrategy(index::SegmentDirectoryBasePtr segDir,
        config::IndexPartitionSchemaPtr schema,
        index::OfflineAttributeSegmentReaderContainerPtr attrReaders,
        const index_base::SegmentMergeInfos& segMergeInfos, const MergePlan& plan);
    ~TimeSeriesSplitStrategy();


public:
    bool Init(const util::KeyValueMap& parameters) override;
    segmentindex_t DoProcess(segmentid_t oldSegId, docid_t oldLocalId) override;

public:
    static const std::string STRATEGY_NAME;

private:
    TimeSeriesSplitProcessor* mTimeSeriesSplitProcessor;
    std::string mAttributeName;
    FieldType mFieldType;


private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TimeSeriesSplitStrategy);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_TIME_SERIES_SPLIT_STRATEGY_H
