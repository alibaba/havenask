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
#include "aios/network/gig/multi_call/metric/IstioMetricReporter.h"
#include <string>

using namespace std;
using namespace kmonitor;

namespace multi_call {

AUTIL_LOG_SETUP(multi_call, IstioMetricReporter);

IstioMetricReporter::IstioMetricReporter(KMonitor *kMonitor)
    : _kMonitor(kMonitor) {

    /////// xds response related
    // report xds response process result in the terms of qps. Use ratio of
    // counter.
    DEFINE_MUTABLE_METRIC(kMonitor, XdsQps, "istio.xds.qps", COUNTER, NORMAL);
    // latency of request to response.
    DEFINE_MUTABLE_METRIC(kMonitor, XdsRequestLatency,
                          "istio.xds.requestLatency", GAUGE, NORMAL);
    // status of resource in XDS response
    DEFINE_MUTABLE_METRIC(kMonitor, XdsResourceProcessed,
                          "istio.xds.resourceProcessed", COUNTER, NORMAL);
    // metric in xds response sanity check
    DEFINE_MUTABLE_METRIC(kMonitor, XdsRspCheck, "istio.xds.responseCheck",
                          COUNTER, NORMAL);
    // the queue length of worker pool processing xds response.
    DEFINE_MUTABLE_METRIC(kMonitor, XdsTaskQueue, "istio.xds.taskQueueLen",
                          GAUGE, NORMAL);

    /////// status related
    // report status of grpc client stream
    DEFINE_MUTABLE_METRIC(kMonitor, XdsBuildStream, "istio.xds.buildStream",
                          COUNTER, NORMAL);
    // report the client xds synced status
    DEFINE_MUTABLE_METRIC(kMonitor, XdsState, "istio.xds.state", GAUGE, NORMAL);
    // number of clusters of CDS/EDS
    DEFINE_MUTABLE_METRIC(kMonitor, XdsSize, "istio.xds.size", GAUGE, NORMAL);
    // size of cache
    DEFINE_MUTABLE_METRIC(kMonitor, XdsCacheSize, "istio.xds.cacheSize", GAUGE,
                          NORMAL);
    // size of subscribed resources
    DEFINE_MUTABLE_METRIC(kMonitor, XdsSubscribeSize, "istio.xds.subSize",
                          GAUGE, NORMAL);
}

IstioMetricReporter::~IstioMetricReporter() {}

} // namespace multi_call
