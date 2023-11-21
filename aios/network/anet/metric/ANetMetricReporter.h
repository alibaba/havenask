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

#include "aios/network/anet/metric/ANetMetricReporterConfig.h"
#include "aios/network/anet/transport.h"
#include "autil/Log.h"
#include "autil/LoopThread.h"

namespace anet {

class ANetMetricReporter {
public:
    ANetMetricReporter(const ANetMetricReporterConfig &config) : _config(config) {}
    virtual ~ANetMetricReporter();

public:
    virtual bool init(Transport *transport);
    void release();

protected:
    void reportThreadProc();
    virtual void doReportConnStat(const ConnStat &stats) = 0;

protected:
    ANetMetricReporterConfig _config;
    Transport *_transport{nullptr};

    std::map<std::string, ConnStat> _lastConnStats;
    autil::LoopThreadPtr _reportThread;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace anet