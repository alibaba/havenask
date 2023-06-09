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
#include "build_service/common/CpuSpeedEstimater.h"

#include <algorithm>
#include <autil/HashAlgorithm.h>
#include <functional>
#include <vector>

#include "build_service/util/Monitor.h"

using namespace std;
using namespace autil;

namespace build_service { namespace common {
BS_LOG_SETUP(common, CpuSpeedEstimater);

const char CpuSpeedEstimater::HASH_STR[] = "DEADBEEF";
const uint64_t CpuSpeedEstimater::HASH_MASK = 0xFFFFF00000000000ULL;
const uint16_t CpuSpeedEstimater::MAX_SAMPLE_COUNT_LIMIT = 30;
const uint16_t CpuSpeedEstimater::SPEED_CIRCULAR_BUFFER_SIZE = 15;
const uint16_t CpuSpeedEstimater::MIN_STAT_WINDOW = 5;
const int64_t CpuSpeedEstimater::INVALID_CPU_SPEED = -1;

CpuSpeedEstimater::CpuSpeedEstimater(indexlib::util::MetricProviderPtr metricProvider)
    : _headCursor(0)
    , _isStarted(false)
    , _metricProvider(metricProvider)
{
    assert(SPEED_CIRCULAR_BUFFER_SIZE > 0);
    _speedCircularBuffer.resize(SPEED_CIRCULAR_BUFFER_SIZE, INVALID_CPU_SPEED);

    _currentSpeedMetric =
        DECLARE_METRIC(_metricProvider, "estimater/cpu_speed.current_speed", kmonitor::STATUS, "count");
    _avgSpeedMetric = DECLARE_METRIC(_metricProvider, "estimater/cpu_speed.avg_speed", kmonitor::STATUS, "count");
    _medianSpeedMetric = DECLARE_METRIC(_metricProvider, "estimater/cpu_speed.median_speed", kmonitor::STATUS, "count");
    _emaSpeedMetric = DECLARE_METRIC(_metricProvider, "estimater/cpu_speed.ema_speed", kmonitor::STATUS, "count");
}

bool CpuSpeedEstimater::Start(uint16_t sampleCountLimit, int64_t checkIntervalInSeconds)
{
    _loopThreadPtr = LoopThread::createLoopThread(std::bind(&CpuSpeedEstimater::WorkLoop, this, sampleCountLimit),
                                                  checkIntervalInSeconds * 1000 * 1000, "cpu_estimater_workloop");
    _isStarted = (_loopThreadPtr.get() != nullptr) ? true : false;
    return _isStarted;
}

void CpuSpeedEstimater::Stop()
{
    _isStarted = false;
    _loopThreadPtr.reset();
    std::fill(_speedCircularBuffer.begin(), _speedCircularBuffer.end(), INVALID_CPU_SPEED);
}

void CpuSpeedEstimater::WorkLoop(uint16_t sampleCountLimit)
{
    int64_t beginTs = autil::TimeUtility::currentTimeInMicroSeconds();
    // prevent too much cpu waste to finish the loop
    if (sampleCountLimit > MAX_SAMPLE_COUNT_LIMIT) {
        BS_LOG(WARN,
               "sample count limit[%d] is larger than the max count limit[%d],"
               " has set sample count to %d.",
               sampleCountLimit, MAX_SAMPLE_COUNT_LIMIT, MAX_SAMPLE_COUNT_LIMIT);
        sampleCountLimit = MAX_SAMPLE_COUNT_LIMIT;
    }
    // _headCursor always point to the oldest speed in circlar buffer.
    _speedCircularBuffer[_headCursor] = GetNewSpeed(sampleCountLimit);
    MoveHeadCursor();
    ReportMetrics();
    int64_t currentTs = autil::TimeUtility::currentTimeInMicroSeconds();
    BS_LOG(INFO, "cpu speed: current[%ld], avg[%ld], median[%ld], ema[%ld], emaNoColdStart[%ld] workloop took %.1f ms.",
           GetCurrentSpeed(), GetAvgSpeed(), GetMedianSpeed(), GetEMASpeed(), GetEMASpeedNoColdStart(),
           float(currentTs - beginTs) / 1000);
    return;
}

bool CpuSpeedEstimater::CollectSpeedSamples(uint16_t sampleCountLimit, vector<int64_t>* speedSamples) const
{
    int64_t beginTs = autil::TimeUtility::currentTimeInMicroSeconds();
    size_t sampleCount = 0;
    size_t hashCount = 0;
    while (sampleCount < sampleCountLimit) {
        hashCount++;
        string key = std::string(HASH_STR) + std::to_string(hashCount);
        uint64_t result = autil::HashAlgorithm::hashString64(key.data(), key.size());
        if (unlikely((HASH_MASK & result) == 0)) {
            int64_t currentTs = autil::TimeUtility::currentTimeInMicroSeconds();
            int64_t speed = hashCount * 1000 * 1000 / (currentTs - beginTs + 1);
            speedSamples->push_back(speed);
            sampleCount++;
        }
    }
    if (speedSamples->empty()) {
        return false;
    }
    return true;
}

void CpuSpeedEstimater::ReportMetrics()
{
    if (_metricProvider) {
        int64_t currentSpeed = GetCurrentSpeed();
        int64_t avgSpeed = GetAvgSpeed();
        int64_t medianSpeed = GetMedianSpeed();
        int64_t emaSpeed = GetEMASpeedNoColdStart();
        if (currentSpeed != INVALID_CPU_SPEED) {
            REPORT_METRIC(_currentSpeedMetric, currentSpeed);
        }
        if (avgSpeed != INVALID_CPU_SPEED) {
            REPORT_METRIC(_avgSpeedMetric, avgSpeed);
        }
        if (medianSpeed != INVALID_CPU_SPEED) {
            REPORT_METRIC(_medianSpeedMetric, medianSpeed);
        }
        if (emaSpeed != INVALID_CPU_SPEED) {
            REPORT_METRIC(_emaSpeedMetric, emaSpeed);
        }
    }
    return;
}

void CpuSpeedEstimater::GetSpeedsInStatWindow(std::vector<int64_t>* speeds) const
{
    size_t headCursor = _headCursor;
    size_t i = headCursor;
    do {
        if (_speedCircularBuffer[i] != INVALID_CPU_SPEED) {
            speeds->push_back(_speedCircularBuffer[i]);
        }
        i = (i + 1) % _speedCircularBuffer.size();
    } while (i != headCursor);

    return;
}

int64_t CpuSpeedEstimater::GetAvgSpeed() const
{
    std::vector<int64_t> tmp;
    GetSpeedsInStatWindow(&tmp);
    if (tmp.size() < MIN_STAT_WINDOW) {
        return INVALID_CPU_SPEED;
    }
    return GetAvg(tmp);
}

int64_t CpuSpeedEstimater::GetAvg(const vector<int64_t>& samples) const
{
    int64_t sum = 0;
    size_t count = 0;
    for (size_t i = 0; i < samples.size(); i++) {
        sum += samples[i];
        count++;
    }
    return sum / count;
}

int64_t CpuSpeedEstimater::GetMedianSpeed() const
{
    std::vector<int64_t> tmp;
    GetSpeedsInStatWindow(&tmp);
    if (tmp.size() < MIN_STAT_WINDOW) {
        return INVALID_CPU_SPEED;
    }
    return GetMedian(&tmp);
}

int64_t CpuSpeedEstimater::GetMedian(std::vector<int64_t>* samples) const
{
    size_t medianIdx = (samples->size() - 1) / 2;
    std::nth_element(samples->begin(), samples->begin() + medianIdx, samples->end(), std::greater<int64_t>());
    return (*samples)[medianIdx];
}

int64_t CpuSpeedEstimater::GetEMASpeed() const
{
    std::vector<int64_t> tmp;
    GetSpeedsInStatWindow(&tmp);
    if (tmp.size() < MIN_STAT_WINDOW) {
        return INVALID_CPU_SPEED;
    }
    return (int64_t)GetEMA(tmp);
}

int64_t CpuSpeedEstimater::GetEMASpeedNoColdStart() const
{
    std::vector<int64_t> tmp;
    GetSpeedsInStatWindow(&tmp);
    if (tmp.size() < 1) {
        return INVALID_CPU_SPEED;
    }
    return (int64_t)GetEMA(tmp);
}

float CpuSpeedEstimater::GetEMA(const vector<int64_t>& samples) const
{
    float preEMA = samples[0];
    float smoothingConstant = (float)2 / (1 + samples.size());
    for (size_t i = 1; i < samples.size(); i++) {
        preEMA = MovingAvg(preEMA, samples[i], smoothingConstant);
    }
    return preEMA;
}

}} // namespace build_service::common
