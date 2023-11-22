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

#include <memory>
#include <stdint.h>

#include "autil/legacy/jsonizable.h"

namespace indexlibv2::config {

class BackgroundTaskConfig final : public autil::legacy::Jsonizable
{
public:
    BackgroundTaskConfig();
    BackgroundTaskConfig(const BackgroundTaskConfig& other);
    BackgroundTaskConfig& operator=(const BackgroundTaskConfig& other);
    ~BackgroundTaskConfig();

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    void DisableAll();
    bool initFromEnv();

    int64_t GetCleanResourceIntervalMs() const;
    int64_t GetRenewFenceLeaseIntervalMs() const;
    int64_t GetDumpIntervalMs() const;
    int64_t GetReportMetricsIntervalMs() const;
    int64_t GetAsyncDumpIntervalMs() const;
    int64_t GetMergeIntervalMs() const;
    int64_t GetSubscribeRemoteIndexIntervalMs() const;

private:
    void SwitchToTestDefault();

private:
    struct Impl;

    std::unique_ptr<Impl> _impl;
};

} // namespace indexlibv2::config
