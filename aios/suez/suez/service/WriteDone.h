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
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/result/Result.h"
#include "suez/sdk/IndexProvider.h"
#include "suez/sdk/WriteResult.h"
#include "suez/service/Service.pb.h"
#include "suez/service/WriteTableAccessLog.h"
namespace kmonitor {
class MetricsReporter;
}

namespace suez {

class WriteDone {
public:
    WriteDone(const WriteRequest *request,
              WriteResponse *response,
              google::protobuf::Closure *rpcDone,
              kmonitor::MetricsReporter *metricsReporter,
              std::shared_ptr<WriteTableAccessLog> accessLog,
              int64_t startTime,
              std::shared_ptr<IndexProvider> indexProvider,
              int totalDocCount);

    ~WriteDone();
    void run(autil::result::Result<WriteResult> result, std::vector<int> docIndexList);

private:
    const WriteRequest *_request;
    WriteResponse *_response;
    google::protobuf::Closure *_rpcDone;
    kmonitor::MetricsReporter *_metricsReporter;
    std::shared_ptr<WriteTableAccessLog> _accessLog;
    int64_t _startTime;
    google::protobuf::RepeatedField<int> *_states;
    std::mutex _mu;
    std::vector<std::string> _errors;
    std::shared_ptr<IndexProvider> _indexProvider;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace suez
