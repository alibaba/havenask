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
#ifndef MULTI_CALL_SERVICE_ISTIOMETRICREPORTER_H_
#define MULTI_CALL_SERVICE_ISTIOMETRICREPORTER_H_

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/util/MetricUtil.h"
#include "kmonitor/client/core/MutableMetric.h"

using namespace kmonitor;

namespace multi_call {

class IstioMetricReporter
{
public:
    explicit IstioMetricReporter(kmonitor::KMonitor *kMonitor);
    ~IstioMetricReporter();
    IstioMetricReporter(const IstioMetricReporter &) = delete;
    IstioMetricReporter &operator=(const IstioMetricReporter &) = delete;

    kmonitor::MetricsTagsPtr GetMetricsTags(const std::map<std::string, std::string> &tags_map) {
        return _kMonitor != NULL ? _kMonitor->GetMetricsTags(tags_map) : kmonitor::MetricsTagsPtr();
    }

private:
    DECLARE_MUTABLE_METRIC(XdsQps);
    DECLARE_MUTABLE_METRIC(XdsRequestLatency);
    DECLARE_MUTABLE_METRIC(XdsResourceProcessed);
    DECLARE_MUTABLE_METRIC(XdsRspCheck);
    DECLARE_MUTABLE_METRIC(XdsTaskQueue);
    DECLARE_MUTABLE_METRIC(XdsBuildStream);
    DECLARE_MUTABLE_METRIC(XdsState);
    DECLARE_MUTABLE_METRIC(XdsSize);
    DECLARE_MUTABLE_METRIC(XdsCacheSize);
    DECLARE_MUTABLE_METRIC(XdsSubscribeSize);

private:
    kmonitor::KMonitor *_kMonitor;

private:
    AUTIL_LOG_DECLARE();
};
MULTI_CALL_TYPEDEF_PTR(IstioMetricReporter);

} // namespace multi_call

#endif // MULTI_CALL_SERVICE_ISTIOMETRICREPORTER_H_
