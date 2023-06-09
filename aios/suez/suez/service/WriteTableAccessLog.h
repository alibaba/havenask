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

#include "suez/sdk/WriteResult.h"
#include "suez/service/Service.pb.h"

namespace suez {

class WriteTableAccessLog final {
public:
    WriteTableAccessLog();
    ~WriteTableAccessLog();

public:
    void collectWriteResult(WriteResult writeResult);

public:
    void collectRPCController(google::protobuf::RpcController *controller);
    void collectRequest(const WriteRequest *request);
    void collectClosure(google::protobuf::Closure *done);
    void collectResponse(const WriteResponse *response);

public:
    friend std::ostream &operator<<(std::ostream &os, const WriteTableAccessLog &accessLog);

private:
    int64_t _entryTime;
    std::string _clientIp;
    std::string _traceId;
    std::string _queryString;
    ErrorCode _errCode = TBS_ERROR_NONE;
    std::string _errMsg;
    WriteResult _writeResult;
};

extern std::ostream &operator<<(std::ostream &os, const WriteTableAccessLog &accessLog);

typedef std::shared_ptr<WriteTableAccessLog> WriteTableAccessLogPtr;

} // namespace suez
