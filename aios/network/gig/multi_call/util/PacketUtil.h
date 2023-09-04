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
#ifndef ISEARCH_MULTI_CALL_PACKETUTIL_H
#define ISEARCH_MULTI_CALL_PACKETUTIL_H

#include "aios/network/anet/packet.h"
#include "aios/network/gig/multi_call/common/common.h"

namespace multi_call {

class PacketUtil
{
public:
    PacketUtil();
    ~PacketUtil();

private:
    PacketUtil(const PacketUtil &);
    PacketUtil &operator=(const PacketUtil &);

public:
    static void deletePacket(anet::Packet *packet);
    static bool isControlPacket(anet::Packet *packet, int32_t &ec);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_PACKETUTIL_H
