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
#ifndef HTTP_ARPC_HTTPRPCCONTROLLERBASE_H
#define HTTP_ARPC_HTTPRPCCONTROLLERBASE_H

#include <string>

#include "aios/network/arpc/arpc/CommonMacros.h"

namespace http_arpc {

class HTTPRPCControllerBase : public RPCController {
public:
    virtual void SetErrorCode(int32_t errorCode) = 0;
    virtual int32_t GetErrorCode() = 0;
    virtual void SetQueueTime(int64_t beginTime) = 0;
    virtual int64_t GetQueueTime() const = 0;
    virtual void SetAddr(const std::string &addr) = 0;
    virtual const std::string &GetAddr() const = 0;
};

} // namespace http_arpc

#endif // HTTP_ARPC_HTTPRPCCONTROLLERBASE_H
