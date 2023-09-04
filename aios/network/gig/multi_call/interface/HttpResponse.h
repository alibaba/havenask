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
#ifndef ISEARCH_MULTI_CALL_HTTPRESPONSE_H
#define ISEARCH_MULTI_CALL_HTTPRESPONSE_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/interface/Response.h"

namespace anet {
class HTTPPacket;
}

namespace multi_call {

typedef std::shared_ptr<anet::HTTPPacket> HTTPPacketPtr;

class HttpResponse : public Response
{
public:
    HttpResponse();
    virtual ~HttpResponse();

public:
    void init(void *data) override;
    size_t size() const override;
    const char *getBody(size_t &length) const;
    const char *getHeader(const char *key) const;
    int getStatusCode() const;
    const HTTPPacketPtr &getPacket() const;
    void fillSpan() override;

protected:
    HTTPPacketPtr _packet;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(HttpResponse);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_HTTPRESPONSE_H
