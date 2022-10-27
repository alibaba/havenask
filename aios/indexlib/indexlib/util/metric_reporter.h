#ifndef __INDEXLIB_ADAPTIVE_COUNTER_H
#define __INDEXLIB_ADAPTIVE_COUNTER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/counter/accumulative_counter.h"
#include "indexlib/util/statistic.h"
#include "indexlib/misc/metric_provider.h"
#include <autil/TimeUtility.h>

IE_NAMESPACE_BEGIN(util);

class InputMetricReporter {
public:
    InputMetricReporter() {}
    void Init(misc::MetricPtr metric)
    {
        mMetric = metric;
    }
    void Record(uint64_t value)
    {
        mStat.Record(value);
    }

    void Report() {
        auto curData = mStat.GetStatData();
        if (mMetric)
        {
            auto deltaSum = curData.sum - mLast.sum;
            auto deltaNum = curData.num - mLast.num;
            mMetric->Report(static_cast<double>(deltaSum) / deltaNum);
        }
        mLast = curData;
    }

private:
    misc::MetricPtr mMetric;
    Statistic mStat;
    Statistic::StatData mLast;
};

class QpsMetricReporter
{
public:
    QpsMetricReporter() = default;
    void Init(misc::MetricPtr metric) { mMetric = metric; }
    void IncreaseQps(int64_t val) { mCounter.Increase(val); }
    void Report()
    {
        auto cur = GetTotalCount();
        if (mMetric)
        {
            mMetric->IncreaseQps(cur - mLast);
        }
        mLast = cur;
    }

    int64_t GetTotalCount() { return mCounter.Get(); }

private:
    AccumulativeCounter mCounter;
    int64_t mLast = 0;
    misc::MetricPtr mMetric;
};


IE_NAMESPACE_END(util);

#endif //__INDEXLIB_ADAPTIVE_COUNTER_H
