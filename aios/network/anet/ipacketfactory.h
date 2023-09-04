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
#ifndef ANET_IPACKET_FACTORY_H_
#define ANET_IPACKET_FACTORY_H_

namespace anet {

class Packet;

/**
 * IPacketFactory is used to construct IPacketStreamer.
 * When ANET read response form peer, it will use IPacketFactory
 * to construct a new packet.
 */
class IPacketFactory {
public:
    virtual ~IPacketFactory() {}

    /**
     * The function is called when ANET need to construct a new
     * Packet. Which type of Packet will be generated in ANET is
     * indicated in this function.
     *
     * @param pcode type of Packet
     * @return address of new Packet. NULL if no Packet generated.
     */
    virtual Packet *createPacket(int pcode) = 0;
};
} // namespace anet

#endif /*IPACKET_FACTORY_H_*/
