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
#ifndef ANET_IPACKETHANDLER_H_
#define ANET_IPACKETHANDLER_H_

namespace anet {
class Packet;

class IPacketHandler {
public:
    enum HPRetCode {
        KEEP_CHANNEL = 0,
        CLOSE_CHANNEL = 1,
        FREE_CHANNEL = 2
    };

    /**
     * This function is used in Client. When ANET receive response,
     * it will call handlePacket() to deal with response packet.
     * User use ANET send requests and use handlePacket() deal
     * with response. This function is Called in
     * Connection::handlePacket().
     * In some exceptions, ControlPacket(such as TimeoutPacket,
     * BadPacket) will be received. Obviously, you should consider
     * all these exceptions in you implementation.
     *
     * @param packet response packet or ControlPacket
     * @param args it's used to indicate which request packet this
     * respose packet corresponding. It's the same as args you used
     * in Connection::posePacket().
     * @return not used temporary.
     */
    virtual HPRetCode handlePacket(Packet *packet, void *args) = 0;

    virtual ~IPacketHandler() {}
};
} // namespace anet

#endif /*IPACHETHANDLER_H_*/
