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
#ifndef ISEARCH_MULTI_CALL_METRICREPORTER_H
#define ISEARCH_MULTI_CALL_METRICREPORTER_H

#include "aios/network/gig/multi_call/common/MetaEnv.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/util/MetricUtil.h"

using namespace kmonitor;

namespace multi_call {

class WorkerMetricReporter {
public:
    WorkerMetricReporter(kmonitor::KMonitor *kMonitor, const MetaEnv &metaEnv);
    ~WorkerMetricReporter();

private:
    WorkerMetricReporter(const WorkerMetricReporter &);
    WorkerMetricReporter &operator=(const WorkerMetricReporter &);

    DECLARE_METRIC(QueueSize);
    DECLARE_METRIC(SubUpdateSuccQps);
    DECLARE_METRIC(SubUpdateFailedQps);
    DECLARE_METRIC(CreateSnapshotSuccQps);
    DECLARE_METRIC(CreateSnapshotFailQps);
    DECLARE_METRIC(UpdateSnapshotQps);

private:
    kmonitor::KMonitor *_kMonitor;
    kmonitor::MetricsTagsPtr _defaultTags;

private:
    AUTIL_LOG_DECLARE();
};
MULTI_CALL_TYPEDEF_PTR(WorkerMetricReporter);
} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_METRICREPORTER_H
