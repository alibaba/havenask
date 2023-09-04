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
#ifndef ISEARCH_MULTI_CALL_METRICREPORTERMANAGER_H
#define ISEARCH_MULTI_CALL_METRICREPORTERMANAGER_H

#include <mutex>
#include <unordered_map>

#include "aios/network/gig/multi_call/common/MetaEnv.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/metric/BizMapMetricReporter.h"
#include "aios/network/gig/multi_call/metric/HeartbeatMetricReporter.h"
#include "aios/network/gig/multi_call/metric/IstioMetricReporter.h"
#include "aios/network/gig/multi_call/metric/LinkMetricReporter.h"
#include "aios/network/gig/multi_call/metric/ReplyInfoCollector.h"
#include "aios/network/gig/multi_call/metric/SnapshotInfoCollector.h"
#include "aios/network/gig/multi_call/metric/WorkerMetricReporter.h"

namespace multi_call {

class MetricReporterManager
{
public:
    MetricReporterManager();
    virtual ~MetricReporterManager();

private:
    MetricReporterManager(const MetricReporterManager &);
    MetricReporterManager &operator=(const MetricReporterManager &);

public:
    // virtual for ut
    virtual bool init(bool isAgent, bool disableMetricReport = false,
                      bool simplifyMetricReport = false);

    void reportSnapshotInfo(const SnapshotInfoCollector &snapInfo);
    void reportReplyInfo(const ReplyInfoCollector &replyInfo);
    BizMetricReporterPtr getBizMetricReporter(const std::string &bizName);

    const WorkerMetricReporterPtr &getWorkerMetricReporter() const;
    const HeartbeatMetricReporterPtr &getHeartbeatMetricReporter() const;
    const IstioMetricReporterPtr &getIstioMetricReporter() const;

    const MetaEnv &getMetaEnv() const {
        return _metaEnv;
    }
    void setMetaEnv(MetaEnv &metaEnv) {
        _metaEnv = metaEnv;
    }

    void reportLinkMetric(const MetaEnv &targetMetaEnv, const std::string &biz,
                          const std::string &targetBiz, const std::string &src,
                          const std::string &srcAb, const std::string &stressTest, int64_t latency,
                          bool timeout, bool error);

    void setReportSampling(int32_t sampling);

private:
    static void initKMonitor();

private:
    static std::once_flag _sKMonitorInitFlag;
    kmonitor::KMonitor *_kMonitor;
    bool _isAgent;
    MetaEnv _metaEnv;
    BizMapMetricReporterPtr _bizMapMetricReporter;
    WorkerMetricReporterPtr _workerMetricReporter;
    HeartbeatMetricReporterPtr _heartbeatReporter;
    IstioMetricReporterPtr _istioMetricReporter;
    LinkMetricReporterPtr _linkMetricReporter;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(MetricReporterManager);
} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_METRICREPORTERMANAGER_H
