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

#include "autil/Lock.h"
#include "indexlib/util/metrics/KmonitorTagvNormalizer.h"
#include "indexlib/util/metrics/MetricReporter.h"
#include "indexlib/util/metrics/Monitor.h"

namespace indexlib { namespace util {

template <typename T>
class TaggedMetricReporterGroup
{
public:
    typedef std::shared_ptr<T> MetricReporterPtr;
    typedef std::map<uint64_t, MetricReporterPtr> ReporterMap;
    typedef std::set<std::string> TagKeySet;
    typedef std::vector<std::string> TagKeyVector;

public:
    TaggedMetricReporterGroup() {}
    ~TaggedMetricReporterGroup() {}

    TaggedMetricReporterGroup(const TaggedMetricReporterGroup&) = delete;
    TaggedMetricReporterGroup& operator=(const TaggedMetricReporterGroup&) = delete;
    TaggedMetricReporterGroup(TaggedMetricReporterGroup&&) = delete;
    TaggedMetricReporterGroup& operator=(TaggedMetricReporterGroup&&) = delete;

public:
    void Init(const util::MetricProviderPtr& metricProvider, const std::string& metricPath,
              const TagKeyVector& limitedTagKeys = TagKeyVector())
    {
        for (auto& key : limitedTagKeys) {
            _limitedTagKeys.insert(key);
        }
        IE_INIT_METRIC_GROUP(metricProvider, TypeMetricItem, metricPath, GetMetricType(), "unit");
        IE_INIT_METRIC_GROUP(metricProvider, ReportLatency, metricPath + "_ReportLatency", kmonitor::GAUGE, "us");
        IE_INIT_METRIC_GROUP(metricProvider, GroupItemCount, metricPath + "_GroupItemCount", kmonitor::GAUGE, "count");
    }

    MetricReporterPtr DeclareMetricReporter(const std::map<std::string, std::string>& tagMap)
    {
        MetricReporterPtr reporter(new T);
        reporter->Init(mTypeMetricItemMetric);
        for (auto kv : tagMap) {
            if (!_limitedTagKeys.empty() && _limitedTagKeys.find(kv.first) == _limitedTagKeys.end()) {
                // invalid tag key, ignore
                continue;
            }
            reporter->AddTag(kv.first, kv.second);
        }
        uint64_t tagHash = reporter->GetTagHashCode();

        autil::ScopedLock lock(_threadMutex);
        auto iter = _reporterMap.find(tagHash);
        if (iter != _reporterMap.end()) {
            reporter = iter->second;
        } else {
            _reporterMap.insert(std::make_pair(tagHash, reporter));
        }
        return reporter;
    }

    void Report()
    {
        auto begin = autil::TimeUtility::currentTime();
        ReporterMap targetMap;
        {
            autil::ScopedLock lock(_threadMutex);
            targetMap = _reporterMap;
        }
        for (auto iter = targetMap.begin(); iter != targetMap.end(); iter++) {
            iter->second->Report();
        }
        size_t itemCount = targetMap.size();
        targetMap.clear();
        CleanUselessReporter();
        auto end = autil::TimeUtility::currentTime();
        IE_REPORT_METRIC(ReportLatency, end - begin);
        IE_REPORT_METRIC(GroupItemCount, itemCount);
    }

    size_t GetReporterSize() const
    {
        autil::ScopedLock lock(_threadMutex);
        return _reporterMap.size();
    }

private:
    void CleanUselessReporter()
    {
        autil::ScopedLock lock(_threadMutex);
        for (auto iter = _reporterMap.begin(); iter != _reporterMap.end();) {
            if (iter->second.use_count() > 1) {
                iter++;
                continue;
            }
            iter = _reporterMap.erase(iter);
        }
    }

    static kmonitor::MetricType GetMetricType() { return kmonitor::GAUGE; }

private:
    IE_DECLARE_METRIC(TypeMetricItem);
    IE_DECLARE_METRIC(ReportLatency);
    IE_DECLARE_METRIC(GroupItemCount);
    ReporterMap _reporterMap;
    TagKeySet _limitedTagKeys;
    mutable autil::RecursiveThreadMutex _threadMutex;

private:
    friend class TaggedMetricReporterGroupTest;
};

template <>
inline kmonitor::MetricType TaggedMetricReporterGroup<QpsMetricReporter>::GetMetricType()
{
    return kmonitor::QPS;
}

///////////////////////////////////////////////////////////////////////////////////////////////
typedef TaggedMetricReporterGroup<InputMetricReporter> InputTaggedMetricReporterGroup;
typedef std::shared_ptr<InputTaggedMetricReporterGroup> InputTaggedMetricReporterGroupPtr;

typedef TaggedMetricReporterGroup<QpsMetricReporter> QpsTaggedMetricReporterGroup;
typedef std::shared_ptr<QpsTaggedMetricReporterGroup> QpsTaggedMetricReporterGroupPtr;

typedef TaggedMetricReporterGroup<CpuSlotQpsMetricReporter> CpuSlotQpsTaggedMetricReporterGroup;
typedef std::shared_ptr<CpuSlotQpsTaggedMetricReporterGroup> CpuSlotQpsTaggedMetricReporterGroupPtr;

typedef TaggedMetricReporterGroup<StatusMetricReporter> StatusTaggedMetricReporterGroup;
typedef std::shared_ptr<StatusTaggedMetricReporterGroup> StatusTaggedMetricReporterGroupPtr;

}} // namespace indexlib::util
