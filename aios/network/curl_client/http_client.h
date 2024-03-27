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
#ifndef CURL_CLIENT_CURL_HTTPCLIENT_H
#define CURL_CLIENT_CURL_HTTPCLIENT_H

#include <cstdlib>
#include <curl/curl.h>
#include <map>
#include <memory>
#include <stdio.h>
#include <string>

#include "autil/Log.h"
#include "autil/ResourcePool.h"
#include "autil/ThreadPool.h"
#include "curl_client/common.h"
#include "curl_client/decoder.h"

#define HTTP_CLIENT_DEFAULT_THREAD_NUM 8
#define HTTP_CLIENT_DEFAULT_CONNECT_TIMEOUT_MS 1000
#define HTTP_CLIENT_DEFAULT_TIMEOUT_MS 1000
#define HTTP_CLIENT_DEFAULT_USER_AGENT "aios/curl_client"
#define HTTP_CLIENT_DEFAULT_AUTH_INFO nullptr

namespace network {

using DecoderFactory = std::function<DecoderPtr()>;
using CurlPrepareFunc = std::function<void(CURL *, const std::string &data)>;
using ResponseCallback = std::function<void(const HttpResponse &)>;

enum class HttpMethod {
    Get,
    Post,
};

struct HttpResource {
    CURL *curl;
    char curlErrorBuf[CURL_ERROR_SIZE];
};

CURL_CLIENT_TYPEDEF_PTR(HttpResource);

class HttpClient {
public:
    HttpClient(int threadNum = HTTP_CLIENT_DEFAULT_THREAD_NUM,
               int connectTimeoutMs = HTTP_CLIENT_DEFAULT_CONNECT_TIMEOUT_MS,
               int timeoutMs = HTTP_CLIENT_DEFAULT_TIMEOUT_MS,
               const std::string &userAgent = HTTP_CLIENT_DEFAULT_USER_AGENT,
               AuthInfoPtr authInfo = nullptr,
               const Header &header = {},
               const QueryParam &params = {});
    virtual ~HttpClient();

    HttpResponse Get(const std::string &uri);
    HttpResponse Post(const std::string &uri, const std::string &data);

    HttpResponse syncRequest(const HttpMethod method, const std::string &uri, const std::string &data = "");
    void asyncRequest(const HttpMethod method,
                      const std::string &uri,
                      const std::string &data = "",
                      const ResponseCallback &callback = nullptr);

private:
    bool IsHttps(const std::string &uri) const;
    std::string ParamEscape(const std::string &key) const;
    std::string appendQueryParam(const std::string &uri) const;
    void performCurlRequest(const HttpMethod method,
                            const std::string &uri,
                            const std::string &data,
                            const ResponseCallback &callback) const;
    HttpResource *createResource() const;
    void destroyResource(HttpResource *resource) const;

private:
    const int connectTimeoutMs;
    const int timeoutMs;
    const Header header;
    const QueryParam params;
    const std::string userAgent;
    const AuthInfoPtr authInfo;

    autil::ThreadPool threadPool;
    std::shared_ptr<autil::ResourcePool<HttpResource>> resourcePool;

    AUTIL_LOG_DECLARE();
};

CURL_CLIENT_TYPEDEF_PTR(HttpClient)

} // namespace network

#endif // CURL_CLIENT_CURL_HTTPCLIENT_H
