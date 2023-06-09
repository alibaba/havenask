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

#include <memory>
#include <thread>

#include "autil/EnvUtil.h"
#include "autil/TimeUtility.h"
#include "autil/metric/ProcessCpu.h"
#include "indexlib/base/Constant.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/metrics/KmonitorTagvNormalizer.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "indexlib/util/metrics/Statistic.h"

namespace indexlib { namespace util {

class InputMetricReporter
{
public:
    InputMetricReporter() noexcept {}
    void Init(const util::MetricPtr& metric) noexcept { _metric = metric; }

    void Record(uint64_t value, bool trace = false) noexcept
    {
        _stat.Record(value);
        if (unlikely(trace)) {
            if (!_metric) {
                return;
            }
            kmonitor::MetricsTags tags;
            if (_kmonTags) {
                _kmonTags->MergeTags(&tags);
            }
            tags.AddTag("trace", "true");
            _metric->Report(&tags, value);
        }
    }

    void Report() noexcept
    {
        auto curData = _stat.GetStatData();
        if (_metric) {
            auto deltaSum = curData.sum - _last.sum;
            auto deltaNum = curData.num - _last.num;
            if (deltaNum > 0) {
                _metric->Report(_kmonTags.get(), static_cast<double>(deltaSum) / deltaNum);
            }
        }
        _last = curData;
    }

    void AddTag(const std::string& k, const std::string& v) noexcept
    {
        if (!_kmonTags) {
            _kmonTags.reset(new kmonitor::MetricsTags);
        }
        _kmonTags->AddTag(k, util::KmonitorTagvNormalizer::GetInstance()->Normalize(v));
    }

    void MergeTags(const kmonitor::MetricsTags& tags) noexcept
    {
        if (!_kmonTags) {
            _kmonTags.reset(new kmonitor::MetricsTags);
        }
        tags.MergeTags(_kmonTags.get());
    }

    uint64_t GetTagHashCode() const noexcept { return _kmonTags != nullptr ? _kmonTags->Hashcode() : 0; }

private:
    util::MetricPtr _metric;
    Statistic _stat;
    Statistic::StatData _last;
    std::shared_ptr<kmonitor::MetricsTags> _kmonTags;
};

class StatusMetricReporter
{
public:
    StatusMetricReporter() noexcept : _value(0) {}
    void Init(const util::MetricPtr& metric) noexcept { _metric = metric; }
    void Record(uint64_t value) noexcept { _value = value; }

    void Report() noexcept
    {
        if (_metric) {
            _metric->Report(_kmonTags.get(), _value);
        }
    }

    void AddTag(const std::string& k, const std::string& v) noexcept
    {
        if (!_kmonTags) {
            _kmonTags.reset(new kmonitor::MetricsTags);
        }
        _kmonTags->AddTag(k, util::KmonitorTagvNormalizer::GetInstance()->Normalize(v));
    }

    uint64_t GetTagHashCode() const noexcept { return _kmonTags != nullptr ? _kmonTags->Hashcode() : 0; }

private:
    util::MetricPtr _metric;
    volatile uint64_t _value;
    std::shared_ptr<kmonitor::MetricsTags> _kmonTags;
};

class QpsMetricReporter
{
public:
    QpsMetricReporter() noexcept = default;
    void Init(const util::MetricPtr& metric) noexcept { _metric = metric; }

    void IncreaseQps(int64_t val, bool trace = false) noexcept
    {
        _counter.Increase(val);
        if (unlikely(trace)) {
            if (!_metric) {
                return;
            }
            kmonitor::MetricsTags tags;
            if (_kmonTags) {
                _kmonTags->MergeTags(&tags);
            }
            tags.AddTag("trace", "true");
            _metric->IncreaseQps(val, &tags);
        }
    }

    void Report() noexcept
    {
        auto cur = GetTotalCount();
        if (_metric) {
            _metric->IncreaseQps(cur - _last, _kmonTags.get());
        }
        _last = cur;
    }

    int64_t GetTotalCount() noexcept { return _counter.Get(); }

    void AddTag(const std::string& k, const std::string& v) noexcept
    {
        if (!_kmonTags) {
            _kmonTags.reset(new kmonitor::MetricsTags);
        }
        _kmonTags->AddTag(k, util::KmonitorTagvNormalizer::GetInstance()->Normalize(v));
    }

    void MergeTags(const kmonitor::MetricsTags& tags) noexcept
    {
        if (!_kmonTags) {
            _kmonTags.reset(new kmonitor::MetricsTags);
        }
        tags.MergeTags(_kmonTags.get());
    }

    uint64_t GetTagHashCode() const noexcept { return _kmonTags != nullptr ? _kmonTags->Hashcode() : 0; }

private:
    AccumulativeCounter _counter;
    volatile int64_t _last = 0;
    util::MetricPtr _metric;
    std::shared_ptr<kmonitor::MetricsTags> _kmonTags;
};

class CpuSlotQpsMetricReporter
{
public:
    CpuSlotQpsMetricReporter() noexcept {}

    void Init(const util::MetricPtr& metric) noexcept
    {
        _metric = metric;

        // when set cpu resource limit by cgroup
        // should set INDEXLIB_QUOTA_CPU_SLOT_NUM = max cpu core num current process use
        // if cpu quota = 300, means max use 3 cpu slot
        _cpuSlotCount = autil::EnvUtil::getEnv("INDEXLIB_QUOTA_CPU_SLOT_NUM", 0);
        if (_cpuSlotCount == 0) {
            // if no env set, will use machine-level cpu core num
            _cpuSlotCount = std::max(std::thread::hardware_concurrency(), (unsigned int)1);
        }
        AddTag("cpu_slot_num", autil::StringUtil::toString(_cpuSlotCount));
        double usage = _procCpu.getUsage();
        (void)usage;
        _latestTimestamp = autil::TimeUtility::currentTime();
    }

    void IncreaseQps(uint64_t value, bool trace = false) noexcept
    {
        assert(!trace);
        _counter.Increase(value);
    }

    void Report() noexcept
    {
        int64_t currentTs = autil::TimeUtility::currentTime();
        auto cur = GetTotalCount();
        double usage = _procCpu.getUsage();
        if (_metric) {
            auto tsGap = currentTs - _latestTimestamp;
            int64_t totalCpuSlotTime = (int64_t)(tsGap * _cpuSlotCount * usage);
            if (totalCpuSlotTime > 0) {
                double convertedCpuSlotNum = static_cast<double>(totalCpuSlotTime) / (1000 * 1000);
                _metric->IncreaseQps((double)(cur - _last) / convertedCpuSlotNum, _kmonTags.get());
            }
        }
        _last = cur;
        _latestTimestamp = currentTs;
    }

    int64_t GetTotalCount() noexcept { return _counter.Get(); }

    void AddTag(const std::string& k, const std::string& v) noexcept
    {
        if (!_kmonTags) {
            _kmonTags.reset(new kmonitor::MetricsTags);
        }
        _kmonTags->AddTag(k, util::KmonitorTagvNormalizer::GetInstance()->Normalize(v));
    }

    void MergeTags(const kmonitor::MetricsTags& tags) noexcept
    {
        if (!_kmonTags) {
            _kmonTags.reset(new kmonitor::MetricsTags);
        }
        tags.MergeTags(_kmonTags.get());
    }

    uint64_t GetTagHashCode() const noexcept { return _kmonTags != nullptr ? _kmonTags->Hashcode() : 0; }

private:
    AccumulativeCounter _counter;
    volatile int64_t _last = 0;
    volatile int64_t _latestTimestamp = 0;
    volatile uint32_t _cpuSlotCount = 0;

    util::MetricPtr _metric;
    std::shared_ptr<kmonitor::MetricsTags> _kmonTags;
    autil::metric::ProcessCpu _procCpu;
};

typedef std::shared_ptr<StatusMetricReporter> StatusMetricReporterPtr;
typedef std::shared_ptr<QpsMetricReporter> QpsMetricReporterPtr;
typedef std::shared_ptr<CpuSlotQpsMetricReporter> CpuSlotQpsMetricReporterPtr;
typedef std::shared_ptr<InputMetricReporter> InputMetricReporterPtr;

}} // namespace indexlib::util
