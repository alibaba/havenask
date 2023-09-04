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
#include <string>
#include <vector>

#include "autil/result/Result.h"
#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "navi/resource/QuerySessionR.h"
#include "sql/ops/util/AsyncCallbackCtxBase.h"

namespace multi_call {
class RequestGenerator;
class Response;
} // namespace multi_call

namespace navi {
class AsyncPipe;
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace kmonitor {
class MetricsReporter;
} // namespace kmonitor

namespace sql {

class GigQuerySessionCallbackCtxR : public navi::Resource, public AsyncCallbackCtxBase {
public:
    GigQuerySessionCallbackCtxR();
    ~GigQuerySessionCallbackCtxR();
    GigQuerySessionCallbackCtxR(const GigQuerySessionCallbackCtxR &) = delete;
    GigQuerySessionCallbackCtxR &operator=(const GigQuerySessionCallbackCtxR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    static const std::string RESOURCE_ID;

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
    const std::shared_ptr<navi::AsyncPipe> &getAsyncPipe() const {
        return _asyncPipe;
    }
    bool start(const ScanOptions &options);
    bool tryReportMetrics(kmonitor::MetricsReporter &opMetricsReporter);
    autil::Result<ResponseVec> stealResult();
    int64_t getSeekTime() const;
    // for test
    virtual void onWaitResponse(autil::Result<ResponseVec> result);

private:
    RESOURCE_DEPEND_DECLARE();

private:
    RESOURCE_DEPEND_ON(navi::QuerySessionR, _querySessionR);

protected:
    std::shared_ptr<navi::AsyncPipe> _asyncPipe;
    MetricCollector _metricsCollector;
    autil::Result<ResponseVec> _result;
};

} // namespace sql
