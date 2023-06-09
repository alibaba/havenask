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
#include "aios/network/gig/multi_call/service/HttpConnectionPool.h"
#include "aios/network/gig/multi_call/service/ConnectionFactory.h"
#include "aios/network/gig/multi_call/service/HttpConnection.h"

using namespace std;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, HttpConnectionPool);

HttpConnectionPool::HttpConnectionPool() {}

HttpConnectionPool::~HttpConnectionPool() {}

bool HttpConnectionPool::init(const ProtocolConfig &config) {
    if (!ConnectionPool::init(config)) {
        return false;
    }
    _connectionFactory.reset(
        new AnetConnectionFactory<HttpConnection>(_transport, _queueSize));
    return true;
}

} // namespace multi_call
