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
#include "aios/network/gig/multi_call/grpc/client/GrpcClientClosure.h"
#include "aios/network/gig/multi_call/service/SearchServiceResource.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GrpcClientClosure);

void GrpcClientClosure::run(bool ok) {
    if (!_callBack->isCopyRequest()) {
        auto ec = MULTI_CALL_ERROR_NONE;
        if (!ok || !_status.ok()) {
            ec = MULTI_CALL_REPLY_ERROR_RESPONSE;
            AUTIL_LOG(INFO,
                      "grpc failed: ec: %d, msg: %s, peer: %s, timeout: %lu",
                      _status.error_code(), _status.error_message().c_str(),
                      _context.peer().c_str(),
                      _callBack->getResource()->getTimeout());
        }
        const auto &serverMetaMap = _context.GetServerTrailingMetadata();
        auto it = serverMetaMap.find(GIG_GRPC_RESPONSE_INFO_KEY);
        string responseInfo;
        if (serverMetaMap.end() != it) {
            const auto &stringRef = it->second;
            responseInfo = string(stringRef.data(), stringRef.length());
        }
        assert(_callBack);
        _callBack->run(stealResponseBuf(), ec, "", responseInfo);
    }
    delete this;
}

} // namespace multi_call
