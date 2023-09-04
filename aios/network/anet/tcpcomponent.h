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
#ifndef ANET_TCPCOMPONENT_H_
#define ANET_TCPCOMPONENT_H_
#include <iosfwd>
#include <stddef.h>
#include <stdint.h>

#include "aios/network/anet/iocomponent.h"

namespace anet {
class Socket;

const int SHRINK_COUNTDOWN = 10;

class TCPComponent : public IOComponent {
    friend class TCPComponentTest_testSocketConnectAndClose_Test;
    friend class TCPComponentTest_testHandleErrorEventConnecting_Test;
    friend class TCPComponentTest_testHandleErrorEventConnected_Test;
    friend class TCPComponentTest_testHandleWriteEvent_Test;
    friend class TCPComponentTest_testHandleWriteEventReconnect_Test;
    friend class TCPComponentTest_testHandleReadEvent_Test;
    friend class TCPComponentTest_testInit_Test;
    friend class TCPComponentTest_testCheckTimeout_Test;
    friend class TCPComponentTest_testSetQosOnConnection_Test;
    friend class TCPComponentTest_testCloseSocketNoLock_Test;
    friend class TCPConnectionTest_testMemLeak_Test;

public:
    TCPComponent(Transport *owner, Socket *socket);
    ~TCPComponent();

    bool init(bool isServer = false);

    Connection *createConnection(IPacketStreamer *streamer, IServerAdapter *adapter = NULL);
    virtual RECONNErr reconnect();

    /*
     * 关闭
     */
    void close();
    void closeConnection(Connection *conn);
    //     void closeSocket();
    //     void closeSocketNoLock();

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

    bool handleErrorEvent();
    /*
     * 得到connection
     *
     * @return TCPConnection
     */
    Connection *getConnection() { return _connection; }

    virtual void dump(std::ostringstream &buf);

    bool checkTimeout(int64_t now);
    bool setState(IOCState state);
    virtual void closeAndSetState();
    bool socketReconnect(int64_t now);

protected:
    /*
     * 连接到socket
     */
    bool socketConnect();

    Connection *_connection;
    int64_t _startConnectTime;

private:
    int _shrinkCount;
    bool _keepIOBuffer;
};
} // namespace anet

#endif /*TCPCOMPONENT_H_*/
