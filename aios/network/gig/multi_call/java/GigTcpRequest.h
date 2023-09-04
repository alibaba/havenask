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
#ifndef ISEARCH_MULTI_CALL_GIGTCPREQUEST_H
#define ISEARCH_MULTI_CALL_GIGTCPREQUEST_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/interface/TcpRequest.h"
#include "aios/network/gig/multi_call/interface/TcpResponse.h"

namespace multi_call {

class GigTcpRequest : public TcpRequest
{
public:
    GigTcpRequest(const std::shared_ptr<google::protobuf::Arena> &arena =
                      std::shared_ptr<google::protobuf::Arena>())
        : TcpRequest(arena) {
    }
    ~GigTcpRequest() {
    }

private:
    GigTcpRequest(const GigTcpRequest &);
    GigTcpRequest &operator=(const GigTcpRequest &);

public:
    bool serializeBody(std::string &body) override {
        return true;
    }
    multi_call::ResponsePtr newResponse() override {
        return multi_call::ResponsePtr(new multi_call::TcpResponse);
    }
};

MULTI_CALL_TYPEDEF_PTR(GigTcpRequest);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGTCPREQUEST_H
