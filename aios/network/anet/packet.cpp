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
#include "aios/network/anet/packet.h"

#include "aios/network/anet/channel.h"
#include "aios/network/anet/common.h"
#include "aios/network/anet/ipackethandler.h"
#include "aios/network/anet/timeutil.h"

BEGIN_ANET_NS();

Packet::Packet() {
    _next = NULL;
    _channel = NULL;
    _expireTime = 0;
    _timeoutMs = 0;
    _packetDequeueCB = NULL;
    memset(&_packetHeader, 0, sizeof(PacketHeader));
}

Packet::~Packet() {}

void Packet::setChannel(Channel *channel) {
    if (channel) {
        _channel = channel;
        setChannelId(channel->getId());
        _channel->setId(getChannelId());
    }
}

void Packet::setPcode(int32_t pcode) { _packetHeader._pcode = pcode; }

int32_t Packet::getPcode() { return _packetHeader._pcode; }

void Packet::setExpireTime(int milliseconds) {
    if (milliseconds < 0) {
        _expireTime = TimeUtil::PRE_MAX;
    } else if (milliseconds == 0) {
        _expireTime = 0;
    } else { /**@todo this design is not testing friendly*/
        _timeoutMs = milliseconds;
        _expireTime = TimeUtil::getTime() + static_cast<int64_t>(milliseconds) * static_cast<int64_t>(1000);
    }
}

void Packet::invokeDequeueCB(void) {
    if (_packetDequeueCB != NULL)
        (void)_packetDequeueCB->handlePacket(this, NULL);
}

END_ANET_NS();
