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
#include "aios/network/gig/multi_call/metric/MetricReporterManager.h"

#include "aios/network/gig/multi_call/util/MetricUtil.h"
#include "kmonitor/client/KMonitorFactory.h"

using namespace std;
using namespace autil;

namespace multi_call {

std::once_flag MetricReporterManager::_sKMonitorInitFlag;

AUTIL_LOG_SETUP(multi_call, MetricReporterManager);

MetricReporterManager::MetricReporterManager() : _kMonitor(NULL), _isAgent(false) {
}

MetricReporterManager::~MetricReporterManager() {
}

bool MetricReporterManager::init(bool isAgent, bool disableMetricReport,
                                 bool simplifyMetricReport) {
    if (disableMetricReport) {
        return true;
    }
    // KMonitorFactory::init should have been called before
    // ignore kmon global tags
    if (!KMonitorFactory::IsStarted()) {
        AUTIL_LOG(WARN, "kmonitor should have been started, may lead to wrong gig metrics");
    }

    // all metric reporter share same kmonitor instance, only init once
    std::call_once(_sKMonitorInitFlag, &MetricReporterManager::initKMonitor);

    _kMonitor = kmonitor::KMonitorFactory::GetKMonitor(GIG_KMONITOR);
    if (nullptr == _kMonitor) {
        AUTIL_LOG(ERROR, "get kmonitor [%s] failed", GIG_KMONITOR.c_str());
        return false;
    }

    if (!_metaEnv.init()) {
        AUTIL_LOG(WARN, "meta env init failed");
    }

    _isAgent = isAgent;
    if (!simplifyMetricReport) {
        _bizMapMetricReporter.reset(new BizMapMetricReporter(_kMonitor, isAgent));
        _workerMetricReporter.reset(new WorkerMetricReporter(_kMonitor, _metaEnv));
        _heartbeatReporter.reset(new HeartbeatMetricReporter(_kMonitor));
        _istioMetricReporter.reset(new IstioMetricReporter(_kMonitor));
        AUTIL_LOG(INFO, "init all metric reporter");
    }

    _linkMetricReporter.reset(new LinkMetricReporter(_kMonitor, _metaEnv));

    AUTIL_LOG(INFO, "metric reporter manager init success");
    return true;
}

void MetricReporterManager::initKMonitor() {
    auto kmonitor = kmonitor::KMonitorFactory::GetKMonitor(GIG_KMONITOR);
    if (kmonitor != nullptr) {
        // set empty service name to avoid prepend service name;
        kmonitor->SetServiceName("");
    }
}

const WorkerMetricReporterPtr &MetricReporterManager::getWorkerMetricReporter() const {
    return _workerMetricReporter;
}

const HeartbeatMetricReporterPtr &MetricReporterManager::getHeartbeatMetricReporter() const {
    return _heartbeatReporter;
}

const IstioMetricReporterPtr &MetricReporterManager::getIstioMetricReporter() const {
    return _istioMetricReporter;
}

BizMetricReporterPtr MetricReporterManager::getBizMetricReporter(const string &bizName) {
    if (!_bizMapMetricReporter) {
        return BizMetricReporterPtr();
    }
    return _bizMapMetricReporter->getBizMetricReporter(bizName);
}

void MetricReporterManager::reportSnapshotInfo(const SnapshotInfoCollector &snapInfo) {
    if (_bizMapMetricReporter) {
        _bizMapMetricReporter->reportSnapshotInfo(snapInfo);
    }
}

void MetricReporterManager::reportReplyInfo(const ReplyInfoCollector &replyInfo) {
    if (_bizMapMetricReporter) {
        _bizMapMetricReporter->reportReplyInfo(replyInfo);
    }
}

void MetricReporterManager::reportLinkMetric(const MetaEnv &targetMetaEnv, const std::string &biz,
                                             const std::string &targetBiz, const std::string &src,
                                             const std::string &srcAb,
                                             const std::string &stressTest, int64_t latency,
                                             bool timeout, bool error) {
    if (!_linkMetricReporter) {
        return;
    }
    _linkMetricReporter->reportMetric(targetMetaEnv, biz, targetBiz, src, srcAb, stressTest,
                                      latency, timeout, error);
}

void MetricReporterManager::setReportSampling(int32_t sampling) {
    if (_linkMetricReporter) {
        _linkMetricReporter->setReportSampling(sampling);
    }
}

} // namespace multi_call
