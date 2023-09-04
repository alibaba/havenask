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
#ifndef ANET_DEFAULT_PACKET_STREAMER_H_
#define ANET_DEFAULT_PACKET_STREAMER_H_

#include "aios/network/anet/ipacketstreamer.h"

namespace anet {

class DefaultPacketStreamer : public IPacketStreamer {
public:
    DefaultPacketStreamer(IPacketFactory *factory);

    ~DefaultPacketStreamer();

    virtual StreamingContext *createContext();

    /**
     * get information about the packet in input data buffer
     *
     * @param input  input data buffer
     * @param header packet header
     * @return retrun true if we get the packet header; return false if not.
     */
    bool getPacketInfo(DataBuffer *input, PacketHeader *header, bool *broken);

    /**
     * decode the data in input buffer to get a packet
     *
     * @param input
     * @param header
     * @return return a packet if we get it. return NULL if not
     */
    Packet *decode(DataBuffer *input, PacketHeader *header);

    /**
     * encode a packet into output data buffer
     *
     * @param packet packet to be encoded
     * @param output output data buffer
     * @return return true if we finished encode. return false if not
     */
    bool encode(Packet *packet, DataBuffer *output);

    bool processData(DataBuffer *dataBuffer, StreamingContext *context);
};
} // namespace anet

#endif /*DEFAULT_PACKET_STREAMER_H_*/
