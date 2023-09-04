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

#include <functional>

#include "autil/ThreadPool.h"
#include "autil/WorkItem.h"
#include "indexlib/index/ann/aitheta2/AithetaIndexConfig.h"
#include "indexlib/index/ann/aitheta2/util/MetricReporter.h"
#include "indexlib/index/ann/aitheta2/AithetaQueryWrapper.h"
#include "indexlib/index/ann/aitheta2/AithetaTerm.h"

namespace indexlibv2::index::ann {
class AithetaIndexReader;
} // namespace indexlibv2::index::ann

namespace indexlibv2::index::ann {

class RecallWorkItem : public autil::WorkItem
{
public:
    RecallWorkItem(std::function<void()> func) : _func(func) {}
    ~RecallWorkItem() {}

public:
    void process() override { _func(); }

private:
    std::function<void()> _func;
};

class AithetaRecallReporter
{
public:
    AithetaRecallReporter(const AithetaIndexConfig& aithetaConfig, const MetricReporterPtr& metricReporter,
                          AithetaIndexReader* indexReader)
        : _timer(0)
        , _frequency(1024)
        , _aithetaConfig(aithetaConfig)
        , _metricReporter(metricReporter)
        , _indexReader(indexReader)
    {
    }

    ~AithetaRecallReporter() { Destory(); }
    AithetaRecallReporter(const AithetaRecallReporter&) = delete;
    AithetaRecallReporter& operator=(const AithetaRecallReporter&) = delete;

public:
    Status Init();
    Status AsnynReport(const AithetaQueries& indexQuery);
    void Destory();

private:
    bool EnableReport() { return !((++_timer) % _frequency); }
    void DoReport(const AithetaQueries& indexQuery, bool onlySearchRt);
    Status LRSearch(const AithetaQueries& lrQuery, bool onlySearchRt, std::unordered_set<docid_t>& lrDocSet);
    static size_t CalcTopK(const AithetaQueries& indexQuery);
    std::shared_ptr<kmonitor::MetricsTags> GetKmonTags(const AithetaQueries& aithetaQueries);
    void MetricReport(const std::shared_ptr<Metric>& metric, const std::shared_ptr<kmonitor::MetricsTags>& tags, float value);

private:
    uint32_t _timer;
    uint32_t _frequency;
    AithetaIndexConfig _aithetaConfig;
    MetricReporterPtr _metricReporter;
    AithetaIndexReader* _indexReader;
    std::shared_ptr<autil::ThreadPool> _threadPool;

    std::shared_ptr<Metric> _overallRecallMetric;
    std::shared_ptr<Metric> _realtimeRecallMetric;
    std::shared_ptr<Metric> _realtimeProportionMetric;
    std::unordered_map<std::string, std::shared_ptr<kmonitor::MetricsTags>> _kmonTagMap;

    static constexpr const uint32_t RECALL_THREAD_NUMBER = 1;
    static constexpr const uint32_t RECALL_THREAD_QUEUE_SIZE = 1;
    static constexpr const char* QUERY_TAG_KEY = "query_tag";

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index::ann