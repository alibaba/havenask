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

#include "aios/network/anet/controlpacket.h"

namespace anet {
ControlPacket ControlPacket::BadPacket(CMD_BAD_PACKET, false);
ControlPacket ControlPacket::TimeoutPacket(CMD_TIMEOUT_PACKET, false);
ControlPacket ControlPacket::ConnectionClosedPacket(CMD_CONNECTION_CLOSED, false);
ControlPacket ControlPacket::ReceiveNewConnectionPacket(CMD_RECEIVE_NEW_CONNECTION, false);
ControlPacket ControlPacket::CloseConnectionPacket(CMD_CLOSE_CONNECTION, false);

ControlPacket::ControlPacket(int c, bool freeDelete) {
    _command = c;
    _freeDelete = freeDelete;
}

void ControlPacket::free() {
    if (_freeDelete) {
        delete this;
    }
}

} // namespace anet
