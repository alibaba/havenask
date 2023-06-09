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
#include "ha3/sql/ops/util/AsyncCallbackCtxBase.h"
#include "ha3/sql/resource/MessageWriter.h"
#include "suez/sdk/RemoteTableWriterParam.h"

namespace kmonitor {
class MetricsReporter;
}
namespace navi {
class AsyncPipe;
}

namespace multi_call {
class QuerySession;
} // namespace multi_call

namespace isearch {
namespace sql {

struct AsyncMessageWriteMetricsCollector {
    std::string name;
    int32_t messageCount = 0;
    int64_t writeLatency = 0;
    bool isSucc = false;
};

class AsyncMessageWriteCallbackCtx : public AsyncCallbackCtxBase,
                                     public std::enable_shared_from_this<AsyncMessageWriteCallbackCtx> {
public:
    AsyncMessageWriteCallbackCtx(std::shared_ptr<navi::AsyncPipe> pipe);
    ~AsyncMessageWriteCallbackCtx();

public:
    virtual void start(std::vector<std::pair<uint16_t, std::string>> msgVec,
                       MessageWriter *msgWriter,
                       int64_t timeoutUs,
                       std::shared_ptr<multi_call::QuerySession> querySession);
    const autil::result::Result<suez::WriteCallbackParam> &getResult() const;
    bool tryReportMetrics(bool isSucc, kmonitor::MetricsReporter &opMetricsReporter) const;
    uint64_t getWriteTime() const;

private:
    void onWriteCallback(autil::result::Result<suez::WriteCallbackParam> result);

private:
    std::shared_ptr<navi::AsyncPipe> _pipe;
    autil::result::Result<suez::WriteCallbackParam> _result;
    AsyncMessageWriteMetricsCollector _metricsCollector;
};

typedef std::shared_ptr<AsyncMessageWriteCallbackCtx> AsyncMessageWriteCallbackCtxPtr;

} // end namespace sql
} // end namespace isearch
