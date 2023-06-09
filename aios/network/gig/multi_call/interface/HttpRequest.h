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
#ifndef ISEARCH_MULTI_CALL_HTTPREQUEST_H
#define ISEARCH_MULTI_CALL_HTTPREQUEST_H

#include "aios/network/anet/httppacket.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/interface/Request.h"

namespace multi_call {

enum HttpMethod { HM_GET = 0, HM_POST, HM_UNKNOWN };

class HttpRequest : public Request {
public:
    HttpRequest(HttpMethod method = HM_GET,
                const std::shared_ptr<google::protobuf::Arena> &arena =
                std::shared_ptr<google::protobuf::Arena>())
        : Request(MC_PROTOCOL_HTTP, arena),
          _method(method), _keepAlive(true) {}
    ~HttpRequest();

private:
    HttpRequest(const HttpRequest &);
    HttpRequest &operator=(const HttpRequest &);

public:
    virtual bool serializeBody(std::string &body) = 0;

public:
    bool serialize() override;
    size_t size() const override { return _body.size(); }
    void setBody(const std::string &body) { _body = body; }
    const std::string &getBody() const { return _body; }
    void setHttpMethod(HttpMethod method) { _method = method; }
    HttpMethod getHttpMethod() const { return _method; }
    void setHost(const std::string &host) { _host = host; }
    const std::string &getHost() const { return _host; }
    void setKeepAlive(bool keepAlive) { _keepAlive = keepAlive; }
    bool isKeepAlive() const { return _keepAlive; }
    void setRequestUri(const std::string &uri) { _uri = uri; }
    const std::string &getRequestUri() const { return _uri; }
    void setRequestHeader(const std::string &key, const std::string &value) {
        _headers[key] = value;
    }
    const std::map<std::string, std::string> &getRequestHeaders() const {
        return _headers;
    }
    anet::HTTPPacket *makeHttpPacket();
    void fillSpan() override;

private:
    std::string _body;
    HttpMethod _method;
    bool _keepAlive;
    std::string _host;
    std::string _uri;
    std::map<std::string, std::string> _headers;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(HttpRequest);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_HTTPREQUEST_H
