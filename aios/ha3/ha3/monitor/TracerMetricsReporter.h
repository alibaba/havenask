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

#include "ha3/common/Tracer.h"
#include "ha3/monitor/SessionMetricsReporter.h"

namespace isearch {
namespace monitor {

class TracerMetricsReporter : public SessionMetricsReporter
{
public:
    TracerMetricsReporter();
    ~TracerMetricsReporter();
private:
    TracerMetricsReporter(const TracerMetricsReporter &);
    TracerMetricsReporter& operator=(const TracerMetricsReporter &);
public:
    void setTracer(common::Tracer* tracer) {
        _tracer = tracer;
    }
private:
    virtual void reportQrsMetrics();
    virtual void reportPhase1ProxyMetrics();
    virtual void reportPhase2ProxyMetrics();
    virtual void reportPhase1SearcherMetrics();
    virtual void reportPhase2SearcherMetrics();
private:
    common::Tracer* _tracer;
private:
};

typedef std::shared_ptr<TracerMetricsReporter> TracerMetricsReporterPtr;

} // namespace monitor
} // namespace isearch
