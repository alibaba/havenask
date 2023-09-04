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
#ifndef HTTP_ARPC_HTTPRPCCONTROLLER_H
#define HTTP_ARPC_HTTPRPCCONTROLLER_H

#include <list>

#include "aios/network/http_arpc/HTTPRPCControllerBase.h"

namespace http_arpc {

class HTTPRPCController : public HTTPRPCControllerBase {
public:
    HTTPRPCController();
    ~HTTPRPCController();

public:
    /* override */ void Reset();
    /* override */ bool Failed() const;
    /* override */ std::string ErrorText() const;
    /* override */ void StartCancel();
    /* override */ void SetFailed(const std::string &reason);
    /* override */ bool IsCanceled() const;
    /* override */ void NotifyOnCancel(RPCClosure *callback);

    /* override */ void SetErrorCode(int32_t errorCode);
    /* override */ int32_t GetErrorCode();
    /* override */ void SetQueueTime(int64_t beginTime);
    /* override */ int64_t GetQueueTime() const;
    /* override */ void SetAddr(const std::string &addr);
    /* override */ const std::string &GetAddr() const;

private:
    std::string _reason;
    std::list<RPCClosure *> _cancelList;
    int32_t _errorCode;
    bool _failed;
    bool _canceled;
    int64_t _queueTime;
    std::string _addr;
};

} // namespace http_arpc

#endif // HTTP_ARPC_HTTPRPCCONTROLLER_H
