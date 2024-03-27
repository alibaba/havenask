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
#ifndef CURL_CLIENT_SSL_LOCK_H
#define CURL_CLIENT_SSL_LOCK_H

#include "autil/Log.h"
#include "curl_client/common.h"

namespace network {

class LibsslGlobalLock {
public:
    LibsslGlobalLock();
    LibsslGlobalLock(const LibsslGlobalLock &) = delete;
    LibsslGlobalLock &operator=(const LibsslGlobalLock &) = delete;
    ~LibsslGlobalLock();
    void init();

private:
    AUTIL_LOG_DECLARE();
};

CURL_CLIENT_TYPEDEF_PTR(LibsslGlobalLock);
} // namespace network
#endif // CURL_CLIENT_CURL_HTTPCLIENT_H