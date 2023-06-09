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
#ifndef ISEARCH_BS_BUILDERMETRICS_H
#define ISEARCH_BS_BUILDERMETRICS_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace util {
class MetricProvider;
class Metric;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
typedef std::shared_ptr<Metric> MetricPtr;
}} // namespace indexlib::util

namespace build_service { namespace builder {

// This class is deprecated and is kept for compatibility.
// Please use indexlib build_document_metrics etc. instead.
class BuilderMetrics
{
public:
    BuilderMetrics() {}
    ~BuilderMetrics();

private:
    BuilderMetrics(const BuilderMetrics&);
    BuilderMetrics& operator=(const BuilderMetrics&);

public:
    bool declareMetrics(indexlib::util::MetricProviderPtr metricProvider, bool isKVorKKV = false);
    void reportMetrics(bool ret, DocOperateType op);
    void reportMetrics(size_t totalMsgCount, size_t consumedMsgCount, DocOperateType op);

public:
    uint32_t getTotalDocCount() const { return _totalDocCount; }

public:
    uint32_t _addDocCount = 0;
    uint32_t _addFailedDocCount = 0;

    uint32_t _updateDocCount = 0;
    uint32_t _updateFailedDocCount = 0;

    uint32_t _deleteDocCount = 0;
    uint32_t _deleteFailedDocCount = 0;

    uint32_t _deleteSubDocCount = 0;
    uint32_t _deleteSubFailedDocCount = 0;

    uint32_t _totalDocCount = 0;

    uint32_t TEST_successDocCount = 0;
    // metrics
    indexlib::util::MetricPtr _buildQpsMetric;

    indexlib::util::MetricPtr _addQpsMetric;
    indexlib::util::MetricPtr _addDocCountMetric;
    indexlib::util::MetricPtr _addFailedQpsMetric;
    indexlib::util::MetricPtr _addFailedDocCountMetric;

    indexlib::util::MetricPtr _updateQpsMetric;
    indexlib::util::MetricPtr _updateDocCountMetric;
    indexlib::util::MetricPtr _updateFailedQpsMetric;
    indexlib::util::MetricPtr _updateFailedDocCountMetric;

    indexlib::util::MetricPtr _deleteQpsMetric;
    indexlib::util::MetricPtr _deleteDocCountMetric;
    indexlib::util::MetricPtr _deleteFailedQpsMetric;
    indexlib::util::MetricPtr _deleteFailedDocCountMetric;

    indexlib::util::MetricPtr _deleteSubQpsMetric;
    indexlib::util::MetricPtr _deleteSubDocCountMetric;
    indexlib::util::MetricPtr _deleteSubFailedQpsMetric;
    indexlib::util::MetricPtr _deleteSubFailedDocCountMetric;

    indexlib::util::MetricPtr _totalDocCountMetric;

private:
    bool _metricsEnabled = false;

    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuilderMetrics);

}} // namespace build_service::builder

#endif // ISEARCH_BS_BUILDERMETRICS_H
