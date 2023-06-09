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
#ifndef ANET_DIRECTPACKET_H_
#define ANET_DIRECTPACKET_H_

#include "aios/network/anet/packet.h"
#include "aios/network/anet/directplaceholder.h"
#include "aios/network/anet/defaultpacket.h"
#include <assert.h>
#include <stddef.h>
#include <stdint.h>

namespace anet {
class DataBuffer;

constexpr uint32_t ANET_DIRECT_PACKET_FLAG = 0x494F5249;  // Magic: IORI
constexpr uint32_t ANET_DIRECT_PACKET_VER = 0x1;

struct DirectPacketHeader {
    uint32_t _flag;
    uint32_t _ver;
    uint64_t _reserved;
    int32_t _msgSize;
    int32_t _payloadSize;
    int32_t _toReceiveSize;
} __attribute__((packed));

static_assert(sizeof(DirectPacketHeader) == 28, "Direct Packet Header version 1 header length");

/* ANET_PACKET_FLAG + DirectPacketHeader */
constexpr int ANET_DIRECT_PACKET_INFO_LEN =
    (int)(sizeof(int32_t) + sizeof(PacketHeader) + sizeof(DirectPacketHeader));

static_assert(ANET_DIRECT_PACKET_INFO_LEN == 44, "ANET_DIRECT_PACKET_INFO_LEN size");

class DirectPayload {
public:
    DirectPayload() : _addr(nullptr), _len(0U) {}
    DirectPayload(const char *addr, uint32_t len) : _addr(addr), _len(len) {}
    const char *getAddr() const { return _addr; }
    uint32_t getLen() const { return _len; }

private:
    const char *_addr;
    uint32_t _len;
};

class DirectPacket : public DefaultPacket {
public:
    DirectPacketHeader &getDirectHeader() { return _directHeader; }
    void setDirectHeader(const DirectPacketHeader &directHeader) { _directHeader = directHeader; }

    const DirectPayload &getPayload() const { return _payload; }
    void setPayload(const DirectPayload &payload) { _payload = payload; }

    bool hasPlaceholder() const { return _hasPlaceholder; }
    const DirectPlaceholder &getPlaceholder() const { return _placeholder; }
    void setPlaceholder(const DirectPlaceholder &placeholder) {
        _placeholder = placeholder;
        _hasPlaceholder = true;
    }
    void setPlaceholder(char *addr, size_t len) {
        assert(!_hasPlaceholder);
        if (nullptr != addr && len > 0UL) {
            setPlaceholder(DirectPlaceholder(addr, len));
        }
    }
    int32_t getPayloadSizeFromHeader() const { return _directHeader._payloadSize; }
    int32_t getToReceiveSizeFromHeader() const { return _directHeader._toReceiveSize; }

    using DefaultPacket::decode;
    bool decode(DataBuffer *input, DirectPacketHeader *header);

private:
    DirectPacketHeader _directHeader;
    DirectPayload _payload;
    DirectPlaceholder _placeholder;
    bool _hasPlaceholder = false;
};

}  // namespace anet

#endif  // ANET_DIRECTPACKET_H_
