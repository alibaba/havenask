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

#include "kmonitor_adapter/Metric.h"
#include "kmonitor_adapter/Monitor.h"
#include "kmonitor_adapter/MonitorFactory.h"
#include "kmonitor_adapter/Reporter.h"
#include "kmonitor_adapter/Time.h"

namespace indexlibv2::index {

// TODO(qisa.cb) is there any one need this? only for debug?
// only use for igraph, but igraph config disable this
class KKVMetricsRecorder : public kmonitor_adapter::Recorder
{
private:
    struct Stat {
        std::atomic_uint_fast64_t sum;
        std::atomic_uint_fast64_t num;
        Stat() : sum(0), num(0) {}
        void record(uint64_t val) noexcept
        {
            sum.fetch_add(val, std::memory_order_relaxed);
            num.fetch_add(1, std::memory_order_relaxed);
        }
        Stat& operator+=(const Stat& other)
        {
            if (this != &other) {
                sum.fetch_add(other.sum.load(std::memory_order_relaxed), std::memory_order_relaxed);
                num.fetch_add(other.num.load(std::memory_order_relaxed), std::memory_order_relaxed);
            }
            return *this;
        }
        Stat& operator=(const Stat& other)
        {
            if (this != &other) {
                sum.store(other.sum.load(std::memory_order_relaxed), std::memory_order_relaxed);
                num.store(other.num.load(std::memory_order_relaxed), std::memory_order_relaxed);
            }
            return *this;
        }
    };

public:
    class Group : public kmonitor_adapter::AlignedThreadData
    {
    public:
        Stat beforeSearchBuildingLatency;
        Stat beforeSearchBuiltLatency;
        Stat searchKKVLatency;
        Stat beforeFetchValueLatency;
        Stat beforeFetchCacheLatency; // including lookup cache and fill result from cache
        Stat updateCacheRatio;

        Stat seekRtSegmentCount;
        Stat seekSegmentCount;
        Stat seekSKeyCount;
        Stat matchedSKeyCount;

        Group(Group* m) : mergedGroup(m) {}
        Group() = default;

        Group& operator+=(const Group& other)
        {
            if (this != &other) {
                beforeSearchBuildingLatency += other.beforeSearchBuildingLatency;
                beforeSearchBuiltLatency += other.beforeSearchBuiltLatency;
                searchKKVLatency += other.searchKKVLatency;
                beforeFetchValueLatency += other.beforeFetchValueLatency;
                beforeFetchCacheLatency += other.beforeFetchCacheLatency;
                updateCacheRatio += other.updateCacheRatio;
                seekRtSegmentCount += other.seekRtSegmentCount;
                seekSegmentCount += other.seekSegmentCount;
                seekSKeyCount += other.seekSKeyCount;
                matchedSKeyCount += other.matchedSKeyCount;
            }
            return *this;
        }

    private:
        void atExit() override
        {
            assert(mergedGroup);
            mergedGroup->beforeSearchBuildingLatency += beforeSearchBuildingLatency;
            mergedGroup->beforeSearchBuiltLatency += beforeSearchBuiltLatency;
            mergedGroup->searchKKVLatency += searchKKVLatency;
            mergedGroup->beforeFetchValueLatency += beforeFetchValueLatency;
            mergedGroup->beforeFetchCacheLatency += beforeFetchCacheLatency;
            mergedGroup->updateCacheRatio += updateCacheRatio;
            mergedGroup->seekRtSegmentCount += seekRtSegmentCount;
            mergedGroup->seekSegmentCount += seekSegmentCount;
            mergedGroup->seekSKeyCount += seekSKeyCount;
            mergedGroup->matchedSKeyCount += matchedSKeyCount;
        }

    private:
        Group* mergedGroup = nullptr;
    };

public:
    KKVMetricsRecorder(kmonitor_adapter::Monitor* monitor, const std::string& tableName);
    ~KKVMetricsRecorder();

public:
    const std::string& name() const override
    {
        static std::string _name("kkv_metrics_recorder");
        return _name;
    }
    void report() override
    {
        Group group;
        _threadData->Fold(
            [](void* entryPtr, void* res) {
                auto group = static_cast<Group*>(res);
                auto threadGroup = static_cast<Group*>(entryPtr);
                (*group) += *threadGroup;
            },
            &group);
        group += mMergedGroup;
#define REPORT_KKV_SINGLE_RECORDER(name, isLatency)                                                                    \
    {                                                                                                                  \
        auto deltaSum =                                                                                                \
            group.name.sum.load(std::memory_order_relaxed) - mLast.name.sum.load(std::memory_order_relaxed);           \
        auto deltaNum =                                                                                                \
            group.name.num.load(std::memory_order_relaxed) - mLast.name.num.load(std::memory_order_relaxed);           \
        if (deltaNum != 0) {                                                                                           \
            if (isLatency) {                                                                                           \
                deltaSum = kmonitor_adapter::time::getMicroSeconds(deltaSum);                                          \
            }                                                                                                          \
            name->report(static_cast<double>(deltaSum) / deltaNum);                                                    \
        }                                                                                                              \
    }

        REPORT_KKV_SINGLE_RECORDER(beforeSearchBuildingLatency, true);
        REPORT_KKV_SINGLE_RECORDER(beforeSearchBuiltLatency, true);
        REPORT_KKV_SINGLE_RECORDER(searchKKVLatency, true);
        REPORT_KKV_SINGLE_RECORDER(beforeFetchValueLatency, true);
        REPORT_KKV_SINGLE_RECORDER(beforeFetchCacheLatency, true);
        REPORT_KKV_SINGLE_RECORDER(updateCacheRatio, true);

        REPORT_KKV_SINGLE_RECORDER(seekRtSegmentCount, false);
        REPORT_KKV_SINGLE_RECORDER(seekSegmentCount, false);
        REPORT_KKV_SINGLE_RECORDER(seekSKeyCount, false);
        REPORT_KKV_SINGLE_RECORDER(matchedSKeyCount, false);

#undef REPORT_KKV_SINGLE_RECORDER
        mLast = group;
    }

    Group* GetThreadGroup()
    {
        auto ptr = getThreadData();
        assert(ptr);
        return static_cast<Group*>(ptr);
    }

private:
    kmonitor_adapter::AlignedThreadData* createThreadData() override { return new Group(&mMergedGroup); }

private:
    Group mMergedGroup;
    Group mLast;

    kmonitor_adapter::MetricPtr beforeSearchBuildingLatency;
    kmonitor_adapter::MetricPtr beforeSearchBuiltLatency;
    kmonitor_adapter::MetricPtr searchKKVLatency;
    kmonitor_adapter::MetricPtr beforeFetchValueLatency;
    // including lookup cache and fill result from cache
    kmonitor_adapter::MetricPtr beforeFetchCacheLatency;
    kmonitor_adapter::MetricPtr updateCacheRatio;

    kmonitor_adapter::MetricPtr seekRtSegmentCount;
    kmonitor_adapter::MetricPtr seekSegmentCount;
    kmonitor_adapter::MetricPtr seekSKeyCount;
    kmonitor_adapter::MetricPtr matchedSKeyCount;
};

} // namespace indexlibv2::index
