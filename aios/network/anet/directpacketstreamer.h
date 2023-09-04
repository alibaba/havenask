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
#ifndef ANET_DIRECT_PACKET_STREAMER_H_
#define ANET_DIRECT_PACKET_STREAMER_H_

#include <stdint.h>

#include "aios/network/anet/defaultpacketstreamer.h"
#include "aios/network/anet/ipacketfactory.h"

namespace anet {
class ChannelPool;
class DataBuffer;
class DirectPacket;
class DirectPayload;
class DirectPlaceholder;
class DirectStreamingContext;
class PacketHeader;
class Socket;
class StreamingContext;
struct DirectPacketHeader;

constexpr int64_t DIRECT_WRITE_PAYLOAD_THRESHOLD = 8 * 1024;

class Packet;

class DirectPacketFactory : public IPacketFactory {
public:
    ~DirectPacketFactory() override {}
    Packet *createPacket(int pcode) override;
    DirectPacket *createDirectPacket(int pcode);
};

class DirectPacketStreamer : public DefaultPacketStreamer {
    friend class DirectPacketStreamerTest_testGetPacketInfo_Test;
    friend class DirectPacketStreamerTest_testGetPlaceholderFromChannel_Test;

public:
    DirectPacketStreamer();
    ~DirectPacketStreamer();
    StreamingContext *createContext() override;

    using DefaultPacketStreamer::encode;
    bool encode(Packet *packet, DataBuffer *output, DirectPayload &payloadToWrite);
    bool processData(DataBuffer *dataBuffer, StreamingContext *context) override;
    bool readPayload(DataBuffer *input,
                     Socket *socket,
                     ChannelPool *channelPool,
                     DirectStreamingContext *context,
                     int &socketReadLen);

private:
    using DefaultPacketStreamer::getPacketInfo;
    bool getPacketInfo(DataBuffer *input, PacketHeader *header, DirectPacketHeader *directHeader, bool *broken);
    const DirectPlaceholder &getPlaceholderFromChannel(ChannelPool *channelPool, uint64_t channelId);
    Packet *decode(DataBuffer *input, PacketHeader *header) override;

private:
    DirectPacketFactory _directPacketFactory;
};

} // namespace anet

#endif // ANET_DIRECT_PACKET_STREAMER_H_
