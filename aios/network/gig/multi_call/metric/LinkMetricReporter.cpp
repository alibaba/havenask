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
#include "aios/network/gig/multi_call/metric/LinkMetricReporter.h"
#include "aios/network/gig/multi_call/common/ControllerParam.h"

using namespace kmonitor;

namespace multi_call {

AUTIL_LOG_SETUP(multi_call, LinkMetricReporter);

LinkMetricReporter::LinkMetricReporter(KMonitor *kMonitor, MetaEnv &metaEnv)
    : _kMonitor(kMonitor), _metaEnv(metaEnv), _reportSampling(0) {
    DEFINE_MUTABLE_METRIC(kMonitor, Qps, "qps", QPS, NORMAL);
    DEFINE_MUTABLE_METRIC(kMonitor, Latency, "latency", SUMMARY, NORMAL);
    DEFINE_MUTABLE_METRIC(kMonitor, ErrorQps, "errorQps", QPS, NORMAL);
    DEFINE_MUTABLE_METRIC(kMonitor, TimeoutQps, "timeoutQps", QPS, NORMAL);
}

LinkMetricReporter::~LinkMetricReporter() {}

void LinkMetricReporter::reportMetric(
    const MetaEnv &targetMetaEnv, const std::string &biz,
    const std::string &targetBiz, const std::string &src,
    const std::string &srcAb, const std::string &stressTest, int64_t latency,
    bool timeout, bool error) {
    if (_reportSampling > 0) {
        if (_counter.fetch_add(1) > _reportSampling) {
            _counter = 0;
        } else {
            return;
        }
    }
    doReportMetric(targetMetaEnv, biz, targetBiz, src, srcAb, stressTest,
                   latency, timeout, error);
}

void LinkMetricReporter::doReportMetric(
    const MetaEnv &targetMetaEnv, const std::string &biz,
    const std::string &targetBiz, const std::string &src,
    const std::string &srcAb, const std::string &stressTest, int64_t latency,
    bool timeout, bool error) {
    if (!_kMonitor) {
        return;
    }
    kmonitor::MetricsTagsPtr linkTags = getLinkMetricsTags(
        targetMetaEnv, biz, targetBiz, src, srcAb, stressTest);
    reportQps(1, linkTags);
    reportLatency(latency / FACTOR_US_TO_MS, linkTags);
    if (timeout) {
        reportTimeoutQps(1, linkTags);
    }
    if (error) {
        reportErrorQps(1, linkTags);
    }
}

kmonitor::MetricsTagsPtr LinkMetricReporter::getLinkMetricsTags(
    const MetaEnv &targetMetaEnv, const std::string &biz,
    const std::string &targetBiz, const std::string &src,
    const std::string &srcAb, const std::string &stressTest) {
    std::map<std::string, std::string> linkTags = _metaEnv.getEnvTags();
    std::map<std::string, std::string> targetMetaEnvTag =
        targetMetaEnv.getTargetTags();
    linkTags.insert(targetMetaEnvTag.begin(), targetMetaEnvTag.end());
    linkTags.emplace(GIG_TAG_BIZ, MetricUtil::normalizeTag(biz));
    linkTags.emplace(GIG_TAG_SRC, src);
    linkTags.emplace(GIG_TAG_SRC_AB, srcAb);
    linkTags.emplace(GIG_TAG_STRESS_TEST, stressTest);

    std::string tBiz = MetricUtil::normalizeTag(targetBiz);
    linkTags.emplace(GIG_TAG_TARGET_BIZ, MetricUtil::normalizeEmpty(tBiz));
    return _kMonitor->GetMetricsTags(linkTags);
}

void LinkMetricReporter::setReportSampling(int32_t sampling) {
    _reportSampling = sampling;
}
} // namespace multi_call
