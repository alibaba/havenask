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
#ifndef ANET_SOCKET_H_
#define ANET_SOCKET_H_
#include <errno.h>
#include "aios/network/anet/addrspec.h"
#include "aios/network/anet/atomic.h"
#include "aios/network/anet/connectionpriority.h"
#include <stdint.h>
#include <sys/socket.h>
#include <sstream>
#include <string>

struct sockaddr_in;

namespace anet {
const int LISTEN_BACKLOG = 256;

  class IOComponent;

class Socket {
public:

    Socket(int family = AF_INET);
    virtual ~Socket();

    bool setAddress (const char *address, const int port);
    /* 
     * Set domain socket address. */
    bool setUnixAddress(const char *unixPath);

    bool setAddrSpec(const char *addrspec);

    bool connect();
    bool reconnect();    

    void close();
    void shutdown();


    bool createUDP();

    /*
     * Set socket fd and ip address
     * @param socketHandle: socket fd
     * @param hostAddress: host address
     */
    void setUp(int socketHandle, struct sockaddr *hostAddress, int family = AF_INET);

    /*
     * get socket fd
     *
     * @return fd
     */
    int getSocketHandle();

    IOComponent *getIOComponent();
    void setIOComponent(IOComponent *ioc);

    virtual int write(const void *data, int len);
    virtual int read(void *data, int len);

    bool setKeepAlive(bool on) {
        return setIntOption(SO_KEEPALIVE, on ? 1 : 0);
    }

    bool setKeepAliveParameter(int idleTime, int keepInterval, int cnt);

    bool setReuseAddress(bool on) {
        return setIntOption(SO_REUSEADDR, on ? 1 : 0);
    }

    bool setSoLinger (bool doLinger, int seconds);

    bool setTcpNoDelay(bool noDelay);

    bool setIntOption(int option, int value);

    bool setSoBlocking(bool on);

    bool checkSocketHandle();

    int setPriority(CONNPRIORITY priority);
    
    int setQosGroup(uint64_t jobid, uint32_t instanceid, uint32_t groupid);

    uint32_t getQosGroup();

    int removeQosGroup();

    int getPriority();

    virtual int getSoError();

    int getPort(bool active = true);

    uint32_t getIpAddress(bool active = true);

    char *getAddr(char *dest, int len, bool active = false);

    static int getLastError() {
        return errno;
    }

    inline bool isAddressValid() {
      return addr.isValidState();
    }

    bool getSockAddr(sockaddr_in &addr, bool active = true);
  
    int getProtocolType(void) {return addr.getProtocolType(); }

    /* Functions restructed from ServerSocket class */
    Socket *accept();
    bool listen(int backlog);

    /* below functions are for statics purpose */
    int64_t getBindErrCnt(){return atomic_read(&_bindErrCnt);}
    int64_t getConnectErrCnt(){return atomic_read(&_connectErrCnt);}
    int64_t getWriteErrCnt(){return atomic_read(&_writeErrCnt);}
    int64_t getReadErrCnt(){return atomic_read(&_readErrCnt);}
    int64_t getAcceptErrCnt(){return atomic_read(&_acceptErrCnt);}
    int64_t getListenErrCnt(){return atomic_read(&_listenErrCnt);}
    static int64_t getAccpetConnCnt(){return atomic_read(&_globalAcceptConnCnt);}

    void dump(const char *leadchar, std::ostringstream &buf) {
        std::string leading(leadchar);
        buf << leading << "socket FD: " << _socketHandle << std::endl;
        char tmp[256];
        getAddr(tmp, 256);
        buf << leading << "address: " << tmp << std::endl;
        buf << leading << "Bind Err: " << getBindErrCnt()
            << "\t" << "Connect Err: " << getConnectErrCnt()
            << "\t" << "Write Err: " << getWriteErrCnt() << std::endl;
        buf << leading << "Read Err: " << getReadErrCnt()
            << "\t" << "Accept Err: " << getAcceptErrCnt()
            << "\t" << "Listen Err: " << getListenErrCnt() << std::endl;
    }

    bool bindLocalAddress(const char *localIp, uint16_t localPort);
    int getProtocolFamily();
    virtual char *getRemoteAddr(char *dest, int len);

protected:
    int _socketHandle;
    uint32_t _qosId;
    IOComponent *_iocomponent;
    AddrSpec addr;
    std::string _addrSpec;

private:
    atomic64_t _bindErrCnt;
    atomic64_t _connectErrCnt;
    atomic64_t _writeErrCnt;
    atomic64_t _readErrCnt;
    atomic64_t _acceptErrCnt;
    atomic64_t _listenErrCnt;
    static atomic64_t _globalAcceptConnCnt;

    int rebindconn();
    void clearCnt();
    void bindErrInc(){atomic_inc(&_bindErrCnt);}
    void connectErrInc(){atomic_inc(&_connectErrCnt);}
    void writeErrInc(){atomic_inc(&_writeErrCnt);}
    void readErrInc(){atomic_inc(&_readErrCnt);}
    void acceptErrInc(){atomic_inc(&_acceptErrCnt);}
    static void acceptConnInc(){atomic_inc(&_globalAcceptConnCnt);}
    static void acceptConnDec(){atomic_dec(&_globalAcceptConnCnt);}
    void listernErrInc(){atomic_inc(&_listenErrCnt);}
};


}

#endif /*SOCKET_H_*/
