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
#include "aios/network/arpc/arpc/metric/KMonitorServerMetricReporter.h"

#include "aios/network/arpc/arpc/metric/ServerRPCStats.h"
#include "kmonitor/client/KMonitorFactory.h"

namespace arpc {

AUTIL_LOG_SETUP(arpc, KMonitorServerMetricReporter);

const std::string kServerKMonitorName = "arpc_server_kmonitor";

KMonitorServerMetricReporter::KMonitorServerMetricReporter(const MetricReporterConfig &config,
                                                           kmonitor::MetricLevel level)
    : ServerMetricReporter(config), _level(level), _defaultTags("arpc_type", _config.type) {}

bool KMonitorServerMetricReporter::init() {
    if (!_enable) {
        return true;
    }

    auto kMonitor = kmonitor::KMonitorFactory::GetKMonitor(kServerKMonitorName);
    if (!kMonitor) {
        _enable = false;
        AUTIL_LOG(WARN, "arpc kmonitor server metric reporter init failed");
        return false;
    }

    kMonitor->SetServiceName("arpc.server");

#define CHECK_METRIC_NULLPTR(metric)                                                                                   \
    if (metric == nullptr) {                                                                                           \
        _enable = false;                                                                                               \
        AUTIL_LOG(WARN, "arpc kmonitor server metric reporter init failed, regist metric %s failed", #metric);         \
        return false;                                                                                                  \
    }

    _serverQps.reset(kMonitor->RegisterMetric("qps", kmonitor::QPS, _level));
    CHECK_METRIC_NULLPTR(_serverQps);

    _errorQps.reset(kMonitor->RegisterMetric("error_qps", kmonitor::QPS, _level));
    CHECK_METRIC_NULLPTR(_errorQps);

    _serverTotalCost.reset(kMonitor->RegisterMetric("total_cost_in_us", kmonitor::SUMMARY, _level));
    CHECK_METRIC_NULLPTR(_serverTotalCost);

    _serverRequestUnpackCost.reset(kMonitor->RegisterMetric("request_unpack_cost", kmonitor::SUMMARY, _level));
    CHECK_METRIC_NULLPTR(_serverRequestUnpackCost);

    _serverRequestPendingCost.reset(kMonitor->RegisterMetric("request_pending_cost", kmonitor::SUMMARY, _level));
    CHECK_METRIC_NULLPTR(_serverRequestPendingCost);

    _serverRequestCallCost.reset(kMonitor->RegisterMetric("request_call_cost", kmonitor::SUMMARY, _level));
    CHECK_METRIC_NULLPTR(_serverRequestCallCost);

    _serverResponsePackCost.reset(kMonitor->RegisterMetric("response_pack_cost", kmonitor::SUMMARY, _level));
    CHECK_METRIC_NULLPTR(_serverResponsePackCost);

    _serverResponseSendCost.reset(kMonitor->RegisterMetric("response_send_cost", kmonitor::SUMMARY, _level));
    CHECK_METRIC_NULLPTR(_serverResponseSendCost);

    _serverResponseIovSize.reset(kMonitor->RegisterMetric("response_iov_size", kmonitor::GAUGE, _level));
    CHECK_METRIC_NULLPTR(_serverResponseIovSize);

    _serverThreadItemCount.reset(kMonitor->RegisterMetric("threadpool_item_count", kmonitor::SUMMARY, _level));
    CHECK_METRIC_NULLPTR(_serverThreadItemCount);

#undef CHECK_METRIC_NULLPTR

    AUTIL_LOG(INFO, "arpc kmonitor server metric reporter init done");
    return true;
}

void KMonitorServerMetricReporter::doReportMetric(ServerRPCStats *stats) {
    if (stats == nullptr || !_enable) {
        return;
    }

    _serverQps->Report(&_defaultTags, 1);
    _serverTotalCost->Report(&_defaultTags, stats->totalCostUs());
    _serverRequestUnpackCost->Report(&_defaultTags, stats->requestUnpackCostUs());
    _serverRequestPendingCost->Report(&_defaultTags, stats->requestPendingCostUs());
    _serverRequestCallCost->Report(&_defaultTags, stats->requestCallCostUs());
    _serverResponsePackCost->Report(&_defaultTags, stats->responsePackCostUs());
    _serverResponseSendCost->Report(&_defaultTags, stats->responseSendCostUs());

    auto &iovSizes = stats->iovSizes();
    for (auto &iov : iovSizes) {
        kmonitor::MetricsTags metricTags(
            {{"device", iov.device}, {"remote_device", iov.remoteDevice}, {"arpc_type", _config.type}});
        _serverResponseIovSize->Report(&metricTags, iov.size);
    }

    if (!stats->success()) {
        kmonitor::MetricsTags tags({{"error", ErrorCode_Name(stats->error())}, {"arpc_type", _config.type}});
        _errorQps->Report(&tags, 1);
    }
    _serverThreadItemCount->Report(stats->threadPoolItemCount());
}

} // namespace arpc