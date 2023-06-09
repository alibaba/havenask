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
#ifndef ANET_UDPCONNECTION_H_
#define ANET_UDPCONNECTION_H_
#include "aios/network/anet/connection.h"

namespace anet {
class IPacketStreamer;
class IServerAdapter;
class Socket;

  class UDPConnection : public Connection {
    /*
     * 构造函数
     */
    UDPConnection(Socket *socket, IPacketStreamer *streamer, IServerAdapter *serverAdapter);

    /*
     * 析造函数
     */
    ~UDPConnection();

    /*
     * 写出数据
     *
     * @return 是否成功
     */
    bool writeData();

    /*
     * 读入数据
     *
     * @return 读入数据
     */
    bool readData();

};

}

#endif /*UDPCONNECTION_H_*/
