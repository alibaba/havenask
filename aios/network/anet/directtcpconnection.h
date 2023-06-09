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
#ifndef ANET_DIRECTTCPCONNECTION_H_
#define ANET_DIRECTTCPCONNECTION_H_

#include "aios/network/anet/tcpconnection.h"
#include "aios/network/anet/directpacket.h"
#include <stddef.h>

namespace anet {

class DirectPacketStreamer;
class DirectStreamingContext;
class IPacketStreamer;
class IServerAdapter;
class Socket;

class DirectTCPConnection : public TCPConnection {
public:
    DirectTCPConnection(Socket *socket, IPacketStreamer *streamer, IServerAdapter *serverAdapter);
    ~DirectTCPConnection();
    bool writeData() override;
    bool readData() override;

private:
    void clearWritingPacket();
    void finishPacketWrite();
    int sendBuffer(int &writeCnt, int &error);
    int sendPayload(int &writeCnt, int &error);

private:
    DirectPacketStreamer *_directStreamer;
    DirectStreamingContext *_directContext;
    size_t _payloadLeftToWrite = 0UL;
    DirectPayload _writingPayload;
    DirectPacket *_writingPacket = nullptr;
};

}  // namespace anet

#endif  // ANET_DIRECTTCPCONNECTION_H_
