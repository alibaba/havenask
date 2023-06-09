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
#ifndef ISEARCH_BS_CPUSPEEDESTIMATER_H
#define ISEARCH_BS_CPUSPEEDESTIMATER_H

#include <autil/LoopThread.h>
#include <indexlib/util/metrics/MetricProvider.h>

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace common {

class CpuSpeedEstimater
{
public:
    static const char HASH_STR[];
    static const uint64_t HASH_MASK;
    static const uint16_t MAX_SAMPLE_COUNT_LIMIT;
    static const uint16_t SPEED_CIRCULAR_BUFFER_SIZE;
    static const uint16_t MIN_STAT_WINDOW;
    static const int64_t INVALID_CPU_SPEED;

    explicit CpuSpeedEstimater(indexlib::util::MetricProviderPtr metricProvider);
    ~CpuSpeedEstimater() { Stop(); }

    bool Start(uint16_t sampleCountLimit, int64_t checkIntervalInSeconds);
    void Stop();
    bool IsStarted() const { return _isStarted; }

    int64_t GetCurrentSpeed() const { return _speedCircularBuffer[GetTailCursor()]; }
    int64_t GetAvgSpeed() const;
    int64_t GetMedianSpeed() const;
    int64_t GetEMASpeed() const;
    int64_t GetEMASpeedNoColdStart() const;

private:
    void WorkLoop(uint16_t sampleCountLimit);
    void MoveHeadCursor() { _headCursor = (_headCursor + 1) % _speedCircularBuffer.size(); }

    size_t GetTailCursor() const
    {
        return (_headCursor + _speedCircularBuffer.size() - 1) % _speedCircularBuffer.size();
    }

    int64_t GetNewSpeed(uint16_t sampleCountLimit) const
    {
        std::vector<int64_t> speedSamples;
        if (!CollectSpeedSamples(sampleCountLimit, &speedSamples)) {
            BS_LOG(WARN, "no cpu speed sample was collected.");
            return INVALID_CPU_SPEED;
        }
        return GetMedian(&speedSamples);
    }

    bool CollectSpeedSamples(uint16_t sampleCount, std::vector<int64_t>* speedSamples) const;
    void GetSpeedsInStatWindow(std::vector<int64_t>* speeds) const;

    int64_t GetAvg(const std::vector<int64_t>& samples) const;
    int64_t GetMedian(std::vector<int64_t>* samples) const;
    float GetEMA(const std::vector<int64_t>& samples) const;

    // The formula for calculating an Exponential Moving Average (EMA) is:
    // X = (K * (C - P)) + P
    // Where:
    // X = Current EMA (i.e. EMA to be calculated)
    // C = Current original data value
    // K = Smoothing Constant ()
    // P = Previous EMA
    // Where:
    // K = Smoothing Constant = 2 /(1 + n)
    // n = number of periods for EMA (i.e. the Window to calculate)
    float MovingAvg(float preEMA, int64_t current, float smoothingConstant) const
    {
        return smoothingConstant * (current - preEMA) + preEMA;
    }

    void ReportMetrics();

    void TEST_PrintHistorySpeed() const
    {
        for (size_t i = 0; i < _speedCircularBuffer.size(); i++) {
            std::cout << _speedCircularBuffer[i] << ", ";
        }
        std::cout << std::endl;
    }
    autil::LoopThreadPtr _loopThreadPtr;
    std::vector<int64_t> _speedCircularBuffer;
    size_t _headCursor;
    bool _isStarted;

    indexlib::util::MetricProviderPtr _metricProvider;
    indexlib::util::MetricPtr _currentSpeedMetric;
    indexlib::util::MetricPtr _avgSpeedMetric;
    indexlib::util::MetricPtr _medianSpeedMetric;
    indexlib::util::MetricPtr _emaSpeedMetric;

    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CpuSpeedEstimater);

}} // namespace build_service::common

#endif // ISEARCH_BS_CPUSPEEDESTIMATER_H
