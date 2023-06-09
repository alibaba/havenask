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
#include "aios/network/gig/multi_call/interface/TcpResponse.h"
#include "aios/network/anet/controlpacket.h"
#include "aios/network/anet/defaultpacket.h"
#include "aios/network/gig/multi_call/util/PacketUtil.h"
#include "autil/legacy/base64.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, TcpResponse);

TcpResponse::TcpResponse() : _packet(NULL, PacketUtil::deletePacket) {}

TcpResponse::~TcpResponse() {}

// response should always free data
void TcpResponse::init(void *data) {
    auto packet = static_cast<anet::DefaultPacket *>(data);
    if (getErrorCode() != MULTI_CALL_ERROR_NONE) {
        _packet.reset(packet);
        return;
    }
    int32_t anetErrorCode = 0;
    if (PacketUtil::isControlPacket(packet, anetErrorCode)) {
        PacketUtil::deletePacket(packet);
        if (anet::ControlPacket::CMD_TIMEOUT_PACKET == anetErrorCode) {
            setErrorCode(MULTI_CALL_REPLY_ERROR_TIMEOUT);
        } else {
            setErrorCode(MULTI_CALL_REPLY_ERROR_RESPONSE,
                         anet::ControlPacket::whatCmd(anetErrorCode));
        }
        return;
    }
    _packet.reset(packet);
    assert(_packet);
    if (!deserializeApp()) {
        setErrorCode(MULTI_CALL_REPLY_ERROR_DESERIALIZE_APP);
        AUTIL_LOG(WARN, "deserialize app failed [%s]", toString().c_str());
    }
}

size_t TcpResponse::size() const {
    size_t length = 0;
    getBody(length);
    return length;
}

const char *TcpResponse::getBody(size_t &length) const {
    if (_packet) {
        return _packet->getBody(length);
    } else {
        return NULL;
    }
}

void TcpResponse::fillSpan() {
    auto &span = getSpan();
    if (span && span->isDebug()) {
        opentelemetry::AttributeMap attrs;
        size_t length = 0;
        auto body = getBody(length);
        if (body != nullptr && length > 0) {
            std::string bodyStr = autil::legacy::Base64EncodeFast(std::string(body, length));
            if (!bodyStr.empty() && bodyStr.length() < opentelemetry::kMaxResponseEventSize) {
                attrs.emplace("response.body", bodyStr);
            }
        }
        span->addEvent("response", attrs);
    }
}

} // namespace multi_call
