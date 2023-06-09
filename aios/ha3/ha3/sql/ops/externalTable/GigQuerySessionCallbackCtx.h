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

#include "autil/result/Result.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/TableDistribution.h"
#include "ha3/sql/ops/util/AsyncCallbackCtxBase.h"

namespace multi_call {
class Response;
class QuerySession;
class RequestGenerator;
} // namespace multi_call

namespace navi {
class AsyncPipe;
} // namespace navi

namespace kmonitor {
class MetricsReporter;
} // namespace kmonitor

namespace isearch {
namespace sql {

class GigQuerySessionCallbackCtx : public std::enable_shared_from_this<GigQuerySessionCallbackCtx>,
                                   public AsyncCallbackCtxBase {
public:
    GigQuerySessionCallbackCtx(std::shared_ptr<multi_call::QuerySession> querySession,
                               std::shared_ptr<navi::AsyncPipe> asyncPipe);
    virtual ~GigQuerySessionCallbackCtx();

public:
    struct MetricCollector {
        int64_t lookupLatency = 0;
    };
    struct ScanOptions {
        std::shared_ptr<multi_call::RequestGenerator> requestGenerator;
    };

private:
    using ResponseVec = std::vector<std::shared_ptr<multi_call::Response>>;

public:
    bool start(const ScanOptions &options);
    bool tryReportMetrics(kmonitor::MetricsReporter &opMetricsReporter);
    autil::Result<ResponseVec> stealResult();
    int64_t getSeekTime() const;
    // for test
    virtual void onWaitResponse(autil::Result<ResponseVec> result);

protected:
    std::shared_ptr<multi_call::QuerySession> _querySession;
    MetricCollector _metricsCollector;
    std::shared_ptr<navi::AsyncPipe> _asyncPipe;
    autil::Result<ResponseVec> _result;
};

} // namespace sql
} // namespace isearch
