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
#include "aios/network/anet/socket.h"
#include "aios/network/anet/udpcomponent.h"

#include "aios/network/anet/iocomponent.h"

namespace anet {
class IPacketStreamer;
class IServerAdapter;
class Transport;

/**
  * 构造函数，由Transport调用。
  *
  * @param owner:       Transport
  * @param socket:      Socket
  * @param streamer:    数据包的双向流，用packet创建，解包，组包。
  * @param serverAdapter:  用在服务器端，当Connection初始化及Channel创建时回调时用
  */
UDPComponent::UDPComponent(Transport *owner, Socket *socket, IPacketStreamer *streamer,
                           IServerAdapter *serverAdapter) : IOComponent(owner, socket) {
    _streamer = streamer;
    _serverAdapter = serverAdapter;
}

/*
 * 析构函数
 */
UDPComponent::~UDPComponent() {}

/*
 * 连接到指定的机器
 *
 * @param  isServer: 是否初始化一个服务器的Connection
 * @return 是否成功
 */
bool UDPComponent::init(bool isServer) {
    if (!isServer) { // 不要connect, 是accept产生的

        if (!_socket->connect()) {
            return false;
        }
    }
    _isServer = isServer;
    return true;
}

/*
 * 关闭
 */
void UDPComponent::close() {}

/**
   * 当有数据可写到时被Transport调用
   *
   * @return 是否成功, true - 成功, false - 失败。
   */
bool UDPComponent::handleWriteEvent() {
    return true;
}

/**
 * 当有数据可读时被Transport调用
 *
 * @return 是否成功, true - 成功, false - 失败。
 */
bool UDPComponent::handleReadEvent() {
    return true;
}


}
