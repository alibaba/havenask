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
#include "aios/network/gig/multi_call/interface/ArpcResponse.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, ArpcResponse);

ArpcResponse::ArpcResponse(const std::shared_ptr<google::protobuf::Arena> &arena) : _message(NULL) {
    setProtobufArena(arena);
}

ArpcResponse::~ArpcResponse() {
    freeProtoMessage(_message);
    _message = NULL;
}

// response should always free data
void ArpcResponse::init(void *data) {
    freeProtoMessage(_message);
    _message = static_cast<google::protobuf::Message *>(data);
    if (!_message || getErrorCode() != MULTI_CALL_ERROR_NONE) {
        return;
    }
    assert(_message);
    if (!deserializeApp()) {
        setErrorCode(MULTI_CALL_REPLY_ERROR_DESERIALIZE_APP);
        AUTIL_LOG(WARN, "deserialize app failed [%s]", toString().c_str());
    }
}

google::protobuf::Message *ArpcResponse::getMessage() const {
    return _message;
}

void ArpcResponse::fillSpan() {
    auto &span = getSpan();
    if (span && span->isDebug() && _message) {
        opentelemetry::AttributeMap attrs;
        std::string messageStr = _message->ShortDebugString();
        if (!messageStr.empty() && messageStr.length() < opentelemetry::kMaxResponseEventSize) {
            attrs.emplace("response.message", messageStr);
        }
        span->addEvent("response", attrs);
    }
}

} // namespace multi_call
