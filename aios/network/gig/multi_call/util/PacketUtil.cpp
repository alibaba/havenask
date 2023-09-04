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
#include "aios/network/gig/multi_call/util/PacketUtil.h"

#include "aios/network/anet/controlpacket.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, PacketUtil);

PacketUtil::PacketUtil() {
}

PacketUtil::~PacketUtil() {
}

bool PacketUtil::isControlPacket(anet::Packet *packet, int32_t &ec) {
    if (!packet) {
        return true;
    }
    if (!packet->isRegularPacket()) {
        auto controlPacket = dynamic_cast<anet::ControlPacket *>(packet);
        if (controlPacket) {
            AUTIL_LOG(WARN, "receive control packet: %s", controlPacket->what());
        }
        ec = controlPacket->getCommand();
        return true;
    }
    return false;
}

void PacketUtil::deletePacket(anet::Packet *packet) {
    if (packet) {
        packet->free();
    }
}

} // namespace multi_call
