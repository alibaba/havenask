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
#ifndef ANET_TCPACCEPTOR_H_
#define ANET_TCPACCEPTOR_H_

namespace anet {

class UDPAcceptor : public UDPComponent {

public:
    /**
     * 构造函数，由Transport调用。
     * 输入:
     *  transport:  运输层对象:::spec: 格式 [upd|tcp]:ip:port
     *  streamer:  数据包的双向流，用packet创建，解包，组包。
     * serverAdapter: 用在服务器端，当Connection初始化及Channel创建时回调时用
     */
    UDPAcceptor(Transport *owner, char *spec, IPacketStreamer *streamer, IServerAdapter *serverAdapter);

    /**
     * 当有数据可读时被Transport调用
     * 返回
     * 是否成功, true - 成功, false - 失败。
     */
    bool handleReadEvent();

    /**
     * 不用
     */
    bool handleWriteEvent() { return false; }
};
} // namespace anet

#endif /*TCPACCEPTOR_H_*/
