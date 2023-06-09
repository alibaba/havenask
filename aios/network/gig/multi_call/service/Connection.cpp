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
#include "aios/network/gig/multi_call/service/Connection.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, Connection);

Connection::Connection(const std::string &spec, ProtocolType type,
                       size_t queueSize)
    : _allocId(genAllocId()), _spec(spec), _type(type), _queueSize(queueSize),
      _callBackThreadPool(nullptr) {}

Connection::~Connection() {}

void Connection::setCallBackThreadPool(
    autil::LockFreeThreadPool *callBackThreadPool) {
    _callBackThreadPool = callBackThreadPool;
}

void Connection::startChildRpc(const RequestPtr &request,
                               const CallBackPtr &callBack) const {
    const opentelemetry::SpanPtr &span = request->getSpan();
    if (span) {
        request->fillSpan();
        span->setAttribute(opentelemetry::kSpanAttrReqContentLength,
                           autil::StringUtil::toString(request->size()));
        span->setAttribute(opentelemetry::kSpanAttrNetPeerIp, getHost());
        // for clientRecv
        callBack->setSpan(span);
    }
}

size_t Connection::genAllocId() {
    static std::atomic<size_t> count = { 0 };
    return count.fetch_add(1);
}

} // namespace multi_call
