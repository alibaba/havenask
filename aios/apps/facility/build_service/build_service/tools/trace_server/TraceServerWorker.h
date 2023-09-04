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

#include "trace_server/TraceService.pb.h"

namespace build_service::tools {

class TraceServerWorker
{
public:
    TraceServerWorker();
    ~TraceServerWorker();
    TraceServerWorker(const TraceServerWorker&) = delete;
    TraceServerWorker& operator=(const TraceServerWorker&) = delete;

public:
    void init();

    void test(const proto::TestRequest* request, proto::TestResponse* response);
    void readSwift(const proto::ReadSwiftRequest* request, proto::ReadSwiftResponse* response);
    void readProcessedSwift(const proto::ReadProcessedSwiftRequest* request, proto::ReadSwiftResponse* response);
    void run();

private:
    class Impl;
    std::unique_ptr<Impl> _impl;
};

} // namespace build_service::tools
