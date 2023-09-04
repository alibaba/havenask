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
#ifndef ANET_CONTROL_PACKET_H_
#define ANET_CONTROL_PACKET_H_
#include "aios/network/anet/packet.h"

namespace anet {

class ControlPacket : public Packet {
public:
    enum {
        CMD_BAD_PACKET = 1,
        CMD_TIMEOUT_PACKET,
        CMD_CONNECTION_CLOSED,      // packet-level: triggered when post packet at a closed connection
        CMD_RECEIVE_NEW_CONNECTION, // connection-level: triggered when tcpacceptor accepts a new connection
        CMD_CLOSE_CONNECTION,       // connection-level: triggered when close the given connection
        CMD_END
    };

    static ControlPacket BadPacket;
    static ControlPacket TimeoutPacket;
    static ControlPacket ConnectionClosedPacket;
    static ControlPacket ReceiveNewConnectionPacket;
    static ControlPacket CloseConnectionPacket;

public:
    /**
     * @param c command code
     * @param freeDelete a flag indicating if this object will be delete when
     * free() is called by user.
     */
    ControlPacket(int c, bool freeDelete = true);

    bool isRegularPacket() { return false; }

    void free();

    void countDataLen() {}

    bool encode(DataBuffer *output) { return false; }

    bool decode(DataBuffer *input, PacketHeader *header) { return false; }

    int getCommand() { return _command; }

    /**
     * get description of this packet
     **/
    const char *what() { return whatCmd(_command); }

public:
    static const char *whatCmd(int command) {
        switch (command) {
        case CMD_BAD_PACKET:
            return "Bad Packet received";
            break;
        case CMD_TIMEOUT_PACKET:
            return "Packet Timeout";
            break;
        case CMD_CONNECTION_CLOSED:
            return "Drop packet due to closing connection";
            break;
        case CMD_RECEIVE_NEW_CONNECTION:
            return "Receive new connection";
        case CMD_CLOSE_CONNECTION:
            return "Close connection";
        default:
            return "NULL";
        }
    }

private:
    int _command;
    bool _freeDelete;
};

} // namespace anet

#endif /*PACKET_H_*/
