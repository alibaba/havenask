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
#ifndef ANET_UDPCOMPONENT_H_
#define ANET_UDPCOMPONENT_H_
#include <tr1/unordered_map>

#include "aios/network/anet/iocomponent.h"
#include "aios/network/anet/common.h"

namespace anet {
  class UDPConnection;

class UDPComponent : public IOComponent {

public:
    /**
     * 构造函数，由Transport调用。
     *
     * @param owner:      Transport
     * @param socket:     Socket
     * @param streamer:   数据包的双向流，用packet创建，解包，组包。
     * @param serverAdapter:  用在服务器端，当Connection初始化及Channel创建时回调时用
     */
    UDPComponent(Transport *owner, Socket *socket, IPacketStreamer *streamer, IServerAdapter *serverAdapter);

    /*
     * 析构函数
     */
    ~UDPComponent();

    /*
        * 初始化
        * 
        * @return 是否成功
        */
    bool init(bool isServer = false);

    /*
     * 关闭
     */
    void close();

    /*
        * 当有数据可写到时被Transport调用
        * 
        * @return 是否成功, true - 成功, false - 失败。
        */
    bool handleWriteEvent();

    /*
     * 当有数据可读时被Transport调用
     *
     * @return 是否成功, true - 成功, false - 失败。
     */
    bool handleReadEvent();

private:
    std::tr1::unordered_map<int, UDPConnection*> _connections;  // UDP连接集合
    IPacketStreamer *_streamer;                             // streamer
    IServerAdapter *_serverAdapter;                         // serveradapter
};
}

#endif /*UDPCOMPONENT_H_*/
