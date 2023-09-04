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
#ifndef ANET_ISERVERADAPTER_H
#define ANET_ISERVERADAPTER_H
#include "aios/network/anet/connection.h"
#include "aios/network/anet/ipackethandler.h"
#include "aios/network/anet/packet.h"

namespace anet {

class IServerAdapter {
public:
    /**
     * This function is used in Server. When ANET receive request,
     * it will call handlePacket() to deal with request packet.
     * This function is Called in Connection::handlePacket().
     * In some exceptions, ControlPacket(such as TimeoutPacket,
     * BadPacket) will be received. Obviously, you should consider
     * all these exceptions in you implementation.
     *
     * @param connection indicate where the request packet comes
     * from, you can use this connection to send a response.
     * @param packet request packet or ControlPacket
     * @return not used temporary.
     */
    virtual IPacketHandler::HPRetCode handlePacket(Connection *connection, Packet *packet) = 0;

    virtual ~IServerAdapter() {}
};
} // namespace anet

#endif /*ISERVERADAPTER_H*/
