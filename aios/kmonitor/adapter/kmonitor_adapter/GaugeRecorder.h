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

#include <atomic>

#include "kmonitor_adapter/Monitor.h"
#include "kmonitor_adapter/Reporter.h"
#include "kmonitor_adapter/ThreadData.h"

namespace kmonitor_adapter {

class GaugeRecorder : public Recorder {
protected:
    struct StatData {
        uint64_t sum = 0;
        uint64_t num = 0;
        StatData() = default;
    };
    struct ThreadCounter : public AlignedThreadData {
        std::atomic_uint_fast64_t sum;
        std::atomic_uint_fast64_t num;
        // During teardown, value will be summed into *merged_sum.
        std::atomic_uint_fast64_t *mergedSum;
        std::atomic_uint_fast64_t *mergedNum;
        ThreadCounter(std::atomic_uint_fast64_t *_mergedSum, std::atomic_uint_fast64_t *_mergedNum)
            : sum(0), num(0), mergedSum(_mergedSum), mergedNum(_mergedNum) {}

        void atExit() override {
            *mergedSum += sum.load();
            *mergedNum += num.load();
        }
    };

public:
    GaugeRecorder() = default;
    GaugeRecorder(const std::string &name, MetricPtr metric);
    GaugeRecorder(Monitor *monitor,
                  const std::string &metricName,
                  kmonitor::MetricLevel level,
                  const Metric::KVVec &tags = Metric::KVVec());
    ~GaugeRecorder();

    const std::string &name() const override { return _name; }

    void record(uint64_t val) {
        auto threadCounter = static_cast<ThreadCounter *>(getThreadData());
        assert(threadCounter);
        threadCounter->sum.fetch_add(val, std::memory_order_relaxed);
        threadCounter->num.fetch_add(1, std::memory_order_relaxed);
    }

    void report() override {
        if (!_metric) {
            return;
        }
        StatData data;
        _threadData->Fold(
            [](void *entryPtr, void *res) {
                auto globalData = static_cast<StatData *>(res);
                auto threadData = static_cast<ThreadCounter *>(entryPtr);
                globalData->sum += threadData->sum.load(std::memory_order_relaxed);
                globalData->num += threadData->num.load(std::memory_order_relaxed);
            },
            &data);
        data.sum += _mergedSum.load(std::memory_order_relaxed);
        data.num += _mergedNum.load(std::memory_order_relaxed);

        auto deltaSum = processValue(data.sum - _last.sum);
        auto deltaNum = data.num - _last.num;

        if (deltaNum != 0) {
            _metric->report(static_cast<double>(deltaSum) / deltaNum);
        }

        _last = data;
    }

    virtual uint64_t processValue(uint64_t val) { return val; }

protected:
    AlignedThreadData *createThreadData() override { return new ThreadCounter(&_mergedSum, &_mergedNum); }

protected:
    std::string _name;
    MetricPtr _metric;

    // counter
    std::atomic_uint_fast64_t _mergedSum{0};
    std::atomic_uint_fast64_t _mergedNum{0};

    StatData _last;
};

} // namespace kmonitor_adapter
