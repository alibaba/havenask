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
#include <cstring>
#include <curl/curl.h>
#include <curl_client/callback_handler.h>
#include <curl_client/http_client.h>
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>
#include <utility>

using namespace std;
using namespace autil;

namespace network {
AUTIL_LOG_SETUP(network, HttpClient);

static const map<HttpMethod, CurlPrepareFunc> CURL_CLIENT_HTTP_METHOD_PREPARE_FUNC_MAP = {
    {HttpMethod::Get, [](CURL *curl, const string &data) { curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L); }},
    {HttpMethod::Post,
     [](CURL *curl, const string &data) {
         curl_easy_setopt(curl, CURLOPT_POST, 1L);
         curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data.c_str());
         curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, data.size());
     }},
};

HttpClient::HttpClient(int threadNum,
                       int connectTimeoutMs,
                       int timeoutMs,
                       const string &userAgent,
                       AuthInfoPtr authInfo,
                       const Header &header,
                       const QueryParam &params)
    : connectTimeoutMs(connectTimeoutMs)
    , timeoutMs(timeoutMs)
    , header(header)
    , params(params)
    , userAgent(userAgent)
    , authInfo(authInfo)
    , threadPool(threadNum) {
    AUTIL_LOG(INFO,
              "threadNum: %d, connectTimeoutMs: %d, timeoutMs: %d, userAgent: %s",
              threadNum,
              connectTimeoutMs,
              timeoutMs,
              userAgent.c_str());
    threadPool.start();
    resourcePool = make_shared<autil::ResourcePool<HttpResource>>(
        threadNum,
        [this]() { return createResource(); },
        [this](HttpResource *resource) { destroyResource(resource); });
}

HttpClient::~HttpClient() {
    threadPool.stop();
    resourcePool.reset();
}

void HttpClient::asyncRequest(const HttpMethod method,
                              const string &uri,
                              const string &data,
                              const ResponseCallback &callback) {
    const auto task = [this, method, uri, data, callback]() { performCurlRequest(method, uri, data, callback); };
    threadPool.pushTask(task);
}

HttpResponse HttpClient::syncRequest(const HttpMethod method, const string &uri, const string &data) {
    HttpResponse result;
    Notifier notifier;
    const auto callback = [&result, &notifier](const HttpResponse &response) {
        if (response.done) {
            result = response;
            notifier.notify();
        }
    };
    asyncRequest(method, uri, data, callback);
    notifier.waitNotification();
    return result;
}

void HttpClient::performCurlRequest(const HttpMethod method,
                                    const string &uri,
                                    const string &data,
                                    const ResponseCallback &callback) const {
    const auto resource = resourcePool->getUnique();
    auto curl = resource->curl;
    CURL_CLIENT_HTTP_METHOD_PREPARE_FUNC_MAP.at(method)(curl, data);
    curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
    auto decoder = make_shared<Decoder>(callback);
    auto curlErrorBuf = resource->curlErrorBuf;

    string url = appendQueryParam(uri);
    string headerString;
    curl_slist *headerList = NULL;

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, CallBackHandler::write_callback);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, CallBackHandler::header_callback);

    // this pointer is passed to the callback function
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &decoder);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &decoder);

    for (Header::const_iterator it = this->header.begin(); it != this->header.end(); ++it) {
        headerString = it->first;
        headerString += ": ";
        headerString += it->second;
        headerList = curl_slist_append(headerList, headerString.c_str());
    }
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headerList);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, this->userAgent.c_str());
    if (this->connectTimeoutMs) {
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, this->connectTimeoutMs);
    }
    if (this->timeoutMs) {
        curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, this->timeoutMs);
    }

    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
    curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, curlErrorBuf);

    // set auth info
    if (IsHttps(url) && this->authInfo && this->authInfo->IsValid()) {
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
        curl_easy_setopt(curl, CURLOPT_CAINFO, authInfo->caCert.c_str());
        curl_easy_setopt(curl, CURLOPT_SSLCERT, authInfo->clientCert.c_str());
        curl_easy_setopt(curl, CURLOPT_SSLCERTTYPE, "PEM");
        curl_easy_setopt(curl, CURLOPT_SSLKEY, authInfo->clientKey.c_str());
        curl_easy_setopt(curl, CURLOPT_SSLKEYTYPE, "PEM");
    } else {
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    }

    const auto curlCode = curl_easy_perform(curl);
    curl_slist_free_all(headerList);

    if (curlCode != CURLE_OK) {
        const auto errorStr = url + " [" + curl_easy_strerror(curlCode) + "]: " + string(curlErrorBuf);
        fill(curlErrorBuf, curlErrorBuf + CURL_ERROR_SIZE, 0);
        AUTIL_LOG(ERROR, "curl error: %s", errorStr.c_str());
        HttpResponse errorResponse;
        errorResponse.curlCode = curlCode;
        errorResponse.body = errorStr;
        callback(errorResponse);
    }
    decoder->DecodeDone();
    curl_easy_reset(curl);
}

HttpResponse HttpClient::Get(const string &url) { return syncRequest(HttpMethod::Get, url, ""); }

HttpResponse HttpClient::Post(const string &url, const string &data) {
    return syncRequest(HttpMethod::Post, url, data);
}

string HttpClient::ParamEscape(const string &key) const {
    string ret;
    char *output = curl_easy_escape(nullptr, key.c_str(), key.size());
    if (output) {
        ret = string(output);
        curl_free(output);
    }
    return ret;
}

string HttpClient::appendQueryParam(const string &uri) const {
    if (this->params.size() == 0) {
        return uri;
    }
    string tmp;
    for (QueryParam::const_iterator iter = params.begin(); iter != params.end(); ++iter) {
        if (tmp == "") {
            tmp = ParamEscape(iter->first) + "=" + ParamEscape(iter->second);
        } else {
            tmp += "&" + ParamEscape(iter->first) + "=" + ParamEscape(iter->second);
        }
    }
    if (tmp.length() > 0) {
        if (uri.find("?") != string::npos) {
            return uri + "&" + tmp;
        } else {
            return uri + "?" + tmp;
        }
    }
    return uri;
}

bool HttpClient::IsHttps(const string &uri) const { return (uri.find("https://") == 0); }

// TODO(wangyin): write these in the constructor of HttpResource
HttpResource *HttpClient::createResource() const {
    HttpResource *ret = new HttpResource();
    const auto curl = curl_easy_init();
    if (!curl) {
        AUTIL_LOG(FATAL, "curl_easy_init failed");
    }
    ret->curl = curl;
    fill(ret->curlErrorBuf, ret->curlErrorBuf + CURL_ERROR_SIZE, 0);
    return ret;
}

void HttpClient::destroyResource(HttpResource *resource) const {
    if (resource) {
        curl_easy_cleanup(resource->curl);
        delete resource;
    }
}

} // namespace network
