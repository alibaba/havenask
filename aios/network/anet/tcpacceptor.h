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
#include <stdint.h>
#include <ostream>

#include "aios/network/anet/iocomponent.h"

namespace anet {
class IOComponent;

class TCPAcceptor : public IOComponent {

public:
    /**
     * @param owner: pointer of a transport object
     * @param socket pointer of a socket object
     * @param timeout: set timeout in millisecond for server sending replies
     */
    TCPAcceptor(Transport *owner, Socket *socket, IPacketStreamer *streamer,
                IServerAdapter *serverAdapter, int timeout, 
                int maxIdleTimeInMillseconds, int backlog);

    bool init(bool isServer = false);

    void close();

    bool handleReadEvent();

    bool handleWriteEvent() {
        return true;
    }

    bool handleErrorEvent(); 

    bool checkTimeout(int64_t now);

    virtual void dump(std::ostringstream &buf) {
        IOComponent::dump(buf);
        buf << "Type: TCPAcceptor" << std::endl;
        buf << "Streamer: " << _streamer << "\tServerAdapter: " << _serverAdapter << std::endl;
        buf << "Max Idle Time: " << _maxIdleTimeInMillseconds << "ms" << std::endl;
        buf << "Queue Timeout: " << _timeout << "ms" << std::endl;
        buf << "Backlog: " << _backlog << std::endl;
    }

    /* for UT purpose */
    IOComponent *getLastAcceptedComponent() {
        return _lastAcceptedComponent;
    }

private:
    IPacketStreamer *_streamer;      
    IServerAdapter  *_serverAdapter; 
    int _timeout;
    int _maxIdleTimeInMillseconds;
    int _backlog;

    /* for testing purpose */
    IOComponent *_lastAcceptedComponent;
};
}

#endif /*TCPACCEPTOR_H_*/
