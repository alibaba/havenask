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

#include <stdint.h>
#include <memory>

#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/proto/BasicDefs.pb.h"

namespace isearch {
namespace monitor {

class SessionMetricsReporter
{
public:
    SessionMetricsReporter();
    virtual ~SessionMetricsReporter();
private:
    SessionMetricsReporter(const SessionMetricsReporter &);
    SessionMetricsReporter& operator=(const SessionMetricsReporter&);
public:
    void setPhaseNumber(uint32_t phaseNumber) {
        _phaseNumber = phaseNumber;
    }
    void setRoleType(proto::RoleType role) { _role = role; }
    void setMetricsCollector(SessionMetricsCollectorPtr collectorPtr)
    {
        _metricsCollectPtr = collectorPtr;
    }
    bool reportMetrics();
private:
    virtual void reportQrsMetrics() = 0;
    virtual void reportPhase1ProxyMetrics() = 0;
    virtual void reportPhase2ProxyMetrics() = 0;
    virtual void reportPhase1SearcherMetrics() = 0;
    virtual void reportPhase2SearcherMetrics() = 0;
protected:
    SessionMetricsCollectorPtr _metricsCollectPtr;
    proto::RoleType _role;
    uint32_t _phaseNumber;
private:
};

typedef std::shared_ptr<SessionMetricsReporter> SessionMetricsReporterPtr;

} // namespace monitor
} // namespace isearch

