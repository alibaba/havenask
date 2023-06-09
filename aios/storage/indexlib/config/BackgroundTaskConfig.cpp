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
#include "indexlib/config/BackgroundTaskConfig.h"

#include "autil/EnvUtil.h"

namespace indexlibv2::config {

AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.config, BackgroundTaskConfig);

struct BackgroundTaskConfig::Impl {
    int64_t cleanResourceIntervalMs = 10 * 1000;        // 10s
    int64_t renewFenceLeaseIntervalMs = 60 * 60 * 1000; // 60min
    int64_t dumpIntervalMs = 60 * 60 * 1000;            // 60min
    int64_t reportMetricsIntervalMs = 10 * 1000;        // 10s
    int64_t asyncDumpIntervalMs = 10 * 1000;            // 10s
    int64_t mergeIntervalMs = 1 * 60 * 1000;            // 1min
    int64_t memReclaimIntervalMs = 1 * 1000;            // 1s
    int64_t subscribeRemoteIndexIntervalMs = 0;         // do not subscribe remote index, for madrox
};

BackgroundTaskConfig::BackgroundTaskConfig() : _impl(std::make_unique<BackgroundTaskConfig::Impl>())
{
    if (autil::EnvUtil::getEnv("IS_TEST_MODE", false)) {
        SwitchToTestDefault();
    }
    _impl->subscribeRemoteIndexIntervalMs =
        autil::EnvUtil::getEnv<int64_t>("SECONDARY_INDEX_SUBSCRIBE_INTERVAL", 0) * 60 * 1000;
}
BackgroundTaskConfig::BackgroundTaskConfig(const BackgroundTaskConfig& other)
    : _impl(std::make_unique<BackgroundTaskConfig::Impl>(*other._impl))
{
}
BackgroundTaskConfig& BackgroundTaskConfig::operator=(const BackgroundTaskConfig& other)
{
    if (this != &other) {
        _impl = std::make_unique<BackgroundTaskConfig::Impl>(*other._impl);
    }
    return *this;
}
BackgroundTaskConfig::~BackgroundTaskConfig() {}

bool BackgroundTaskConfig::initFromEnv()
{
    // 样本场景使用 避免partition太多导致处理不过来task无限提交内存泄漏，临时修复方案
    // 待线上config支持批量修改后删除改代码 其它场景切勿使用by 142608
    auto configStr = autil::EnvUtil::getEnv("INDEXLIB_BACKGROUND_TASK_CONFIG", "");
    if (!configStr.empty()) {
        try {
            autil::legacy::FromJsonString(*this, configStr);
        } catch (const std::exception& e) {
            AUTIL_LOG(ERROR, "parse background_task_config from: %s failed, error: %s", configStr.c_str(), e.what());
            return false;
        }
    }
    return true;
}

void BackgroundTaskConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("clean_resource_interval_ms", _impl->cleanResourceIntervalMs, _impl->cleanResourceIntervalMs);
    json.Jsonize("renew_fence_lease_interval_ms", _impl->renewFenceLeaseIntervalMs, _impl->renewFenceLeaseIntervalMs);
    if (_impl->renewFenceLeaseIntervalMs >= 0 && _impl->renewFenceLeaseIntervalMs < 60 * 60 * 1000) {
        // renew fence lease interval less than 1 hour is forbidden for fs pressure consideration
        _impl->renewFenceLeaseIntervalMs = 60 * 60 * 1000;
    }
    json.Jsonize("dump_interval_ms", _impl->dumpIntervalMs, _impl->dumpIntervalMs);
    json.Jsonize("report_metrics_interval_ms", _impl->reportMetricsIntervalMs, _impl->reportMetricsIntervalMs);
    json.Jsonize("async_dump_interval_ms", _impl->asyncDumpIntervalMs, _impl->asyncDumpIntervalMs);
    json.Jsonize("merge_interval_ms", _impl->mergeIntervalMs, _impl->mergeIntervalMs);
    json.Jsonize("mem_reclaim_interval_ms", _impl->memReclaimIntervalMs, _impl->memReclaimIntervalMs);
    json.Jsonize("subscribe_remote_index_interval_ms", _impl->subscribeRemoteIndexIntervalMs,
                 _impl->subscribeRemoteIndexIntervalMs);
}

int64_t BackgroundTaskConfig::GetCleanResourceIntervalMs() const { return _impl->cleanResourceIntervalMs; }
int64_t BackgroundTaskConfig::GetRenewFenceLeaseIntervalMs() const { return _impl->renewFenceLeaseIntervalMs; }
int64_t BackgroundTaskConfig::GetDumpIntervalMs() const { return _impl->dumpIntervalMs; }
int64_t BackgroundTaskConfig::GetReportMetricsIntervalMs() const { return _impl->reportMetricsIntervalMs; }
int64_t BackgroundTaskConfig::GetAsyncDumpIntervalMs() const { return _impl->asyncDumpIntervalMs; }
int64_t BackgroundTaskConfig::GetMergeIntervalMs() const { return _impl->mergeIntervalMs; }
int64_t BackgroundTaskConfig::GetMemReclaimIntervalMs() const { return _impl->memReclaimIntervalMs; }
int64_t BackgroundTaskConfig::GetSubscribeRemoteIndexIntervalMs() const
{
    return _impl->subscribeRemoteIndexIntervalMs;
}

void BackgroundTaskConfig::SwitchToTestDefault()
{
    _impl->cleanResourceIntervalMs = 10;
    _impl->dumpIntervalMs = 10;
    _impl->reportMetricsIntervalMs = 10;
    _impl->asyncDumpIntervalMs = 10;
    _impl->mergeIntervalMs = 10;
    _impl->memReclaimIntervalMs = 10;
    _impl->subscribeRemoteIndexIntervalMs = 0;
}

void BackgroundTaskConfig::DisableAll()
{
    static constexpr int64_t INVALID_INTERVAL = -1;
    _impl->cleanResourceIntervalMs = INVALID_INTERVAL;
    _impl->renewFenceLeaseIntervalMs = INVALID_INTERVAL;
    _impl->dumpIntervalMs = INVALID_INTERVAL;
    _impl->reportMetricsIntervalMs = INVALID_INTERVAL;
    _impl->asyncDumpIntervalMs = INVALID_INTERVAL;
    _impl->mergeIntervalMs = INVALID_INTERVAL;
    _impl->memReclaimIntervalMs = INVALID_INTERVAL;
    _impl->subscribeRemoteIndexIntervalMs = INVALID_INTERVAL;
}

} // namespace indexlibv2::config
