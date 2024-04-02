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
#ifndef CURL_CLIENT_BASETYPE_H
#define CURL_CLIENT_BASETYPE_H

#include <algorithm>
#include <cstdlib>
#include <curl/curl.h>
#include <map>
#include <memory>
#include <sstream>
#include <string>

#include "curl_client/common.h"
#include "rapidjson/document.h"

namespace network {

typedef std::map<std::string, std::string> Header;
typedef std::map<std::string, std::string> QueryParam;

struct HttpResponse {
    bool done;
    int curlCode; // 0: success
    int httpCode;
    Header headers;
    std::string body;
};

struct AuthInfo {
    std::string caCert;
    std::string clientCert;
    std::string clientKey;
    bool IsValid() { return (caCert.size() != 0 && clientCert.size() != 0 && clientKey.size() != 0); }
};

CURL_CLIENT_TYPEDEF_PTR(AuthInfo)

struct UploadObject {
    const char *data;
    size_t length;
};

} // namespace network

#endif // CURL_CLIENT_BASETYPE_H
