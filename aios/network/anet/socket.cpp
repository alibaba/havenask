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

#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <assert.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <sys/un.h>
#include <unistd.h>

#include "aios/network/anet/stats.h"
#include "aios/network/anet/log.h"
#include "aios/network/anet/filecontrol.h"
#include "aios/network/anet/e2eqos.h"
#include "aios/network/anet/globalflags.h"
#include "aios/network/anet/addrspec.h"
#include "aios/network/anet/atomic.h"
#include "aios/network/anet/connectionpriority.h"
#include "aios/network/anet/debug.h"
#include "aios/network/anet/ilogger.h"

namespace anet {

atomic64_t Socket::_globalAcceptConnCnt = {0};

Socket::Socket(int family) {
    _socketHandle = -1;
    _iocomponent = NULL;
    _qosId = 0;
    addr.setProtocolFamily(family);
    addr.setProtocolType((int)SOCK_STREAM);
    clearCnt();
}

Socket::~Socket() {
    close();
}

bool Socket::setAddress (const char *address, const int port) {
    char buf[10] = {0,};
    snprintf(buf, 9, ":%d", port);
    _addrSpec = nullptr == address ? "localhost" : address;
    _addrSpec += buf;
    int rc = addr.setInetAddr((int)SOCK_STREAM, address, port);
    return !rc;
}

bool Socket::setAddrSpec (const char *address) {
    _addrSpec = address;
    int rc = addr.setAddrSpec(address);
    return !rc;
}

bool Socket::setUnixAddress(const char *unixPath){
    _addrSpec = unixPath;
    int rc = addr.setUnixAddr((int)SOCK_STREAM, unixPath);
    return !rc;
}

bool Socket::checkSocketHandle() {
    if (_socketHandle != -1) {
        return true;
    }

    if ((_socketHandle = socket(addr.getProtocolFamily(), 
                                addr.getProtocolType(), 0)) == -1) {
        return false;
    }
    if (!FileControl::setCloseOnExec(_socketHandle)) {
        close();
        return false;
    }
    return true;
}

bool Socket::connect() {
    int rc = 0;
    if (!isAddressValid() || !checkSocketHandle()) {
        return false;
    }
    
    if (addr.getProtocolFamily() == AF_UNIX) {
        struct sockaddr_un un;
        memset(&un, 0, sizeof(un));
        un.sun_family = AF_UNIX;
        sprintf(un.sun_path, "%s%d", DOMAIN_SOCK_PATH, getpid());
        /* we need to make sure the file is not there already. */
        unlink(un.sun_path);

        rc = ::bind(_socketHandle,(struct sockaddr *)&un, sizeof(un));
        if (rc < 0){
            bindErrInc();
            ANET_LOG(WARN, "Bind domain socket failed in connect. path:%s, err: %d\n",
                          addr.getUnixPath(), rc);
            return false;
        }
    }

    if (flags::getEnableSocketNonBlockConnect()) {
        setSoBlocking(false);
    }
    rc = ::connect(_socketHandle, addr.getAddr(), addr.getAddrSize());

    // EADDRNOTAVAIL
    if (rc == -1 && errno == EADDRNOTAVAIL && addr.getProtocolFamily() != AF_UNIX) {
        rc = rebindconn();
    }
    
    /* Since the socket is in non-blocking mode, connect will most likely 
     * return EINPROGRESS. This error is not beyond our expectation so
     * we only log the errors other than that. */
    if (rc < 0 && errno != EINPROGRESS) {
        connectErrInc();
        ANET_LOG(ERROR, "connect socket failed, err: %d\n",errno);
    } else if (getPort(_socketHandle) == addr.getPort()) {
        ::close(_socketHandle);
        _socketHandle = -1;
        rc = -1;
    }

    return (0 == rc);
}

int Socket::rebindconn() {
    int                rc = -1;

    if (_socketHandle != -1) {
        ::close(_socketHandle);
        _socketHandle = -1;
    }
    if (checkSocketHandle()) {
        setSoLinger(false, 0);
        setIntOption(SO_KEEPALIVE, 1);
        setTcpNoDelay(true);
        setSoBlocking(false);
        setReuseAddress(true);
        rc = ::connect(_socketHandle, addr.getAddr(), addr.getAddrSize());
        ANET_LOG(WARN, "rebindconn: %d, rc: %d", _socketHandle, rc);
    }
    return rc;
}

bool Socket::reconnect() {
    close();
    return connect();
}

void Socket::close() {
    if (_socketHandle != -1) {
        char tmp[256];
        getAddr(tmp, 256);
        ANET_LOG(DEBUG, "Closing socket, fd=%d, addr=%s", _socketHandle, tmp);
        ::close(_socketHandle);
        acceptConnDec();
        _socketHandle = -1;
    }
}

void Socket::shutdown() {
    if (_socketHandle != -1) {
        ::shutdown(_socketHandle, SHUT_WR);
        acceptConnDec();
    }
}

bool Socket::createUDP() {
    close();
    _socketHandle = socket(AF_INET, SOCK_DGRAM, 0);
    return (_socketHandle != -1);
}

void Socket::setUp(int socketHandle, struct sockaddr *hostAddress, int family) {
    close();
    _socketHandle = socketHandle;
    /* TODO: hard code STREAM type only. */
    addr.setAddr(family, (int)SOCK_STREAM, hostAddress);
}

int Socket::getSocketHandle() {
    return _socketHandle;
}

IOComponent *Socket::getIOComponent() {
    return _iocomponent;
}

void Socket::setIOComponent(IOComponent *ioc) {
    _iocomponent = ioc;
}

int Socket::write(const void *data, int len) {
    if (_socketHandle == -1) {
        return -1;
    }
    if (data == NULL)
        return -1;

    int res = -1;
    do {
        res = ::write(_socketHandle, data, len);
        if (res > 0)
        {
            ANET_COUNT_DATA_WRITE(res);
        }
        else if (-1 == res && (errno != EINTR && errno != EAGAIN))
        {
            writeErrInc();
        }
    } while (res < 0 && errno == EINTR);
    return res;
}

int Socket::read (void *data, int len) {
    if (_socketHandle == -1) {
        return -1;
    }

    if (data == NULL)
       return -1;

    int res = -1;
    do {
        res = ::read(_socketHandle, data, len);
        if (res > 0)
        {
            ANET_COUNT_DATA_READ(res);
        }
        else if (-1 == res && (errno != EINTR && errno != EAGAIN))
        {
            readErrInc();
        }
    } while (-1 == res && errno == EINTR);
    return res;
}

bool Socket::setKeepAliveParameter(int idleTime, int keepInterval, int cnt) {
    if (checkSocketHandle()) {
        int rc = setsockopt(_socketHandle, SOL_TCP, TCP_KEEPIDLE, &idleTime, sizeof(idleTime));
        if (rc != 0)
        {
            ANET_LOG(WARN, "Can not set keep alive idle time for the socket: %d, err %d", 
                             _socketHandle, rc);
            return false;
        }
        rc = setsockopt(_socketHandle, SOL_TCP, TCP_KEEPINTVL, &keepInterval, sizeof(keepInterval));
        if (rc != 0)
        {
            ANET_LOG(WARN, "Can not set keep alive interval for the socket: %d, err %d", 
                             _socketHandle, rc);
            return false;
        }
        rc = setsockopt(_socketHandle, SOL_TCP, TCP_KEEPCNT, &cnt, sizeof(cnt));
        if (rc != 0)
        {
            ANET_LOG(WARN, "Can not set keep alive cnt for the socket: %d, err %d", 
                             _socketHandle, rc);
            return false;
        }
        return true;
    }
    return false;
}

bool Socket::setIntOption (int option, int value) {
    bool rc=false;
    if (checkSocketHandle()) {
        rc = (setsockopt(_socketHandle, SOL_SOCKET, option,
                         (const void *)(&value), sizeof(value)) == 0);
    }
    return rc;
}

bool Socket::setSoLinger(bool doLinger, int seconds) {
    bool rc=false;
    struct linger lingerTime;
    lingerTime.l_onoff = doLinger ? 1 : 0;
    lingerTime.l_linger = seconds;


  
    if (checkSocketHandle()) {
        rc = (setsockopt(_socketHandle, SOL_SOCKET, SO_LINGER,
                         (const void *)(&lingerTime), sizeof(lingerTime)) == 0);
    }

    return rc;
}

bool Socket::setTcpNoDelay(bool noDelay) {
    bool rc = false;
    int noDelayInt = noDelay ? 1 : 0;
    if (checkSocketHandle()) {
        rc = (setsockopt(_socketHandle, IPPROTO_TCP, TCP_NODELAY,
                         (const void *)(&noDelayInt), sizeof(noDelayInt)) == 0);
    }
    return rc;
}

bool Socket::setSoBlocking(bool blockingEnabled) {
    bool rc=false;

    if (checkSocketHandle()) {
        int flags = fcntl(_socketHandle, F_GETFL, NULL);
        if (flags >= 0) {
            if (blockingEnabled) {
                flags &= ~O_NONBLOCK; // clear nonblocking
            } else {
                flags |= O_NONBLOCK;  // set nonblocking
            }

            if (fcntl(_socketHandle, F_SETFL, flags) >= 0) {
                rc = true;
            }
        }
    }

    if (rc == false) {
       ANET_LOG(ERROR, "setSoBlocking false, fd: %d, blockingEnabled: %d\n",
                        _socketHandle, blockingEnabled)
    }

    return rc;
}

int Socket::getPriority() {
    int rc = -1;
    if (checkSocketHandle()) {
        int actualPrio = 0;
        socklen_t prioLen = sizeof(actualPrio);
        rc = getsockopt(_socketHandle, SOL_SOCKET, SO_PRIORITY, &actualPrio, (socklen_t*)&prioLen);
        if (rc < 0) { 
            rc = errno;
            ANET_LOG(ERROR, "Can not get priority for the socket: %d, errno %d", 
                             _socketHandle, rc );
            return rc;
        }
        return actualPrio;
    }
    else 
        ANET_LOG(WARN, "Can not get priority because the socket is invalid. ");
    return rc;
}

int Socket::setPriority(CONNPRIORITY priority) {
    int rc = -1;
    if (checkSocketHandle()) {
        /* SO_PRIORITY seems not working as man page indicated, and IP_TOS
         * field can not get set. But this is ok since linux kernel will 
         * put the packet in to a specific priority queue according to 
         * priority value. Check /sbin/tc -s qdisc output for the detailed
         * mapping.
         */
        rc = setsockopt(_socketHandle,  SOL_SOCKET, SO_PRIORITY,
                         (const void *)(&priority), sizeof(priority)) ;
        if (rc < 0) { 
            rc = errno;
            ANET_LOG(ERROR, "Can not set priority for the socket: %d, err %s", 
                             _socketHandle, strerror(rc));
        }
       
        /* We don't need to set IP_TOS for now. */
#if 0
        int actualPrio = 0;
        socklen_t prioLen = sizeof(actualPrio);
        rc = getsockopt(_socketHandle, SOL_SOCKET, SO_PRIORITY, &actualPrio, (socklen_t*)&prioLen);
        ANET_LOG(WARN, "Socket priority is set to %d, rc %d", actualPrio, rc);

        rc = setsockopt(_socketHandle, SOL_IP, IP_TOS, 
                          (const void *)(&priority), sizeof(priority));
        if (rc < 0) { 
            rc = errno;
            ANET_LOG(ERROR, "Can not set ip tos for the socket: %d, errno %d", 
                             _socketHandle, rc );
        }
#endif

    }
    else 
        ANET_LOG(WARN, "Can not set priority because the socket is still invalid. ");
    return rc;
}
int Socket::setQosGroup(uint64_t jobid, uint32_t instanceid, uint32_t groupid){
    int ret=0;
    if(_socketHandle != -1){
        ret = _ge2eQos.AddGroup(_socketHandle, groupid, jobid, instanceid);
    }
    _qosId = groupid;
    return ret;
}
uint32_t Socket::getQosGroup(){
    return _qosId;
}
int Socket::removeQosGroup(){
    if(_qosId != 0 && _socketHandle != -1)
        return _ge2eQos.RemoveGroup(_socketHandle);
    return 0;
}
char *Socket::getRemoteAddr(char *dest, int len) {
    if (addr.getProtocolFamily() == AF_INET) {
        struct sockaddr_in address;  
        socklen_t addrLen = sizeof(address);
        int ret = getpeername(_socketHandle, (sockaddr*)&address, &addrLen);
        if (ret != 0 || addrLen != sizeof(address) ) {
            ERRSTR(errno);
            ANET_LOG(WARN, "getpeername() fail! [ret, len, fd]: [%d, %d, %d]."
                     "%s! will return user set address.", ret, addrLen, _socketHandle, errStr);
            if (_addrSpec != "") {
                snprintf(dest, len, "%s", _addrSpec.c_str());
                return dest;
            }
            return NULL;
        }

        unsigned long ad = ntohl(address.sin_addr.s_addr);
        snprintf(dest, len, "%d.%d.%d.%d:%d",
                 static_cast<int>((ad >> 24) & 255),
                 static_cast<int>((ad >> 16) & 255),
                 static_cast<int>((ad >> 8) & 255),
                 static_cast<int>(ad & 255),
                 ntohs(address.sin_port));
        return dest;
    }
    else
    {
        snprintf(dest, len, "%s", "unknown");
        return NULL;
    }
}

char *Socket::getAddr(char *dest, int len, bool active) {
    if (addr.getProtocolFamily() == AF_INET) {
        struct sockaddr_in addr;    
        if (!getSockAddr(addr, active)) {
            return NULL;
        }

        unsigned long ad = ntohl(addr.sin_addr.s_addr);
        snprintf(dest, len, "%d.%d.%d.%d:%d",
                 static_cast<int>((ad >> 24) & 255),
                 static_cast<int>((ad >> 16) & 255),
                 static_cast<int>((ad >> 8) & 255),
                 static_cast<int>(ad & 255),
                 ntohs(addr.sin_port));
    }
    else {
        /* Domain socket. */
        snprintf(dest, len, "%s",addr.getUnixPath() );
        dest[len-1] = '\0';
    }
    return dest;
}

int Socket::getPort(bool active) {
    struct sockaddr_in addr;    
    if (getSockAddr(addr, active)) {
        return ntohs(addr.sin_port);
    } else {
        return -1;
    }
}

uint32_t Socket::getIpAddress(bool active) {
    struct sockaddr_in addr;
    if (getSockAddr(addr, active)) {
        return ntohl(addr.sin_addr.s_addr);
    } else {
        return (uint32_t)-1;
    }
}

bool Socket::getSockAddr(sockaddr_in &address, bool active) {
    socklen_t addrLen = sizeof(address);
    if (active) {
        int ret = getsockname(_socketHandle, (sockaddr*)&address, &addrLen);
        if (ret != 0 || addrLen != sizeof(address) ) {
            ERRSTR(errno);
            ANET_LOG(WARN, "getsockname() fail! [ret, len, fd]: [%d, %d, %d]."
                     "%s!", ret, addrLen, _socketHandle, errStr);
            return false;
        }
    } else {
        address = *addr.getInAddr();
    }
    return true;
}

int Socket::getSoError () {
    if (_socketHandle == -1) {
        return EINVAL;
    }

    int lastError = Socket::getLastError();
    int  soError = 0;
    socklen_t soErrorLen = sizeof(soError);
    if (getsockopt(_socketHandle, SOL_SOCKET, SO_ERROR, (void *)(&soError), &soErrorLen) != 0) {
        return lastError;
    }
    if (soErrorLen != sizeof(soError))
        return EINVAL;

    return soError;
}

/* Functions restructed from ServerSocket class */
Socket *Socket::accept() {
    Socket *handleSocket = NULL;

    UnitedAddr address;
    int addrLen = addr.getAddrSize();
    int fd = ::accept(_socketHandle, (struct sockaddr *)&address, (socklen_t *)&addrLen);
    if (fd >= 0) {
        if (flags::getMaxConnectionCount() > 0 && getAccpetConnCnt() >= flags::getMaxConnectionCount())
        {
            ::close(fd);
            ANET_LOG(ERROR, "connection %ld is more than %ld", getAccpetConnCnt(), flags::getMaxConnectionCount());
            return NULL;
        }
        handleSocket = new Socket();
        assert(handleSocket);
        handleSocket->setUp(fd, (struct sockaddr *)&address, addr.getProtocolFamily());

        /* special processing for domain socket. */
        if (addr.getProtocolFamily() == AF_UNIX){
            unlink(address.un.sun_path);
            ANET_LOG(DEBUG, "In accept, unlink domain socket file %s\n", address.un.sun_path);
        }
        acceptConnInc();
    } else {
        int error = getLastError();
        if (error != EAGAIN) {
            acceptErrInc();
            ERRSTR(error);
            ANET_LOG(ERROR, "%s(%d)", errStr, error);
        }
    }

    return handleSocket;
}

bool Socket::listen(int backlog) {
    if (!isAddressValid()) {
        return false;
    }

    if (!checkSocketHandle()) {
        return false;
    }

    setReuseAddress(true);

    if (addr.getProtocolFamily() == AF_UNIX){
        /* special logic for unix domain socket. 
         * we need to make sure the domain socket file does not exist already. 
         * the semantics is similar to ReuseAddress. */
        unlink(addr.getUnixPath());
    }
    if (::bind(_socketHandle, addr.getAddr(), addr.getAddrSize()) < 0)
    {
        int error = getLastError();
        ERRSTR(error);
        ANET_LOG(ERROR, "%s(%d)", errStr, error);
        bindErrInc();
        return false;
    }

    if (::listen(_socketHandle, backlog) < 0) {
        int error = getLastError();
        ERRSTR(error);
        ANET_LOG(ERROR, "%s(%d)", errStr, error);
        listernErrInc();
        return false;
    }

    return true;
}

/* below functions are for statics purpose */
void Socket::clearCnt()
{
    atomic_set(&_bindErrCnt, 0);
    atomic_set(&_connectErrCnt, 0);
    atomic_set(&_writeErrCnt, 0);
    atomic_set(&_readErrCnt, 0);
    atomic_set(&_acceptErrCnt, 0);
    atomic_set(&_listenErrCnt, 0);
}

bool Socket::bindLocalAddress(const char *localIp, uint16_t localPort) {
    ANET_LOG(DEBUG, "localIp:%s, localPort:%d", localIp, localPort);
    if (!checkSocketHandle()) {
        return false;
    }

    sockaddr_in  localAddr;
    memset(static_cast<void *>(&localAddr), 0, sizeof(localAddr));
    localAddr.sin_family = AF_INET;
    localAddr.sin_port = htons(static_cast<short>(localPort));
    if (localIp == NULL || localIp[0] == '\0') {   
        localAddr.sin_addr.s_addr = htonl(INADDR_ANY);
    } else {
        int ret = inet_aton(localIp, &localAddr.sin_addr);
        if (ret == 0) {
            ANET_LOG(ERROR, "Localip [%s] is invalid ip address", localIp);
            return false;
        }
    }
    if (::bind(_socketHandle, (struct sockaddr *)&localAddr,
               sizeof(localAddr)) < 0) 
    {
        bindErrInc();
        int error = getLastError();
        ANET_LOG(ERROR, "Fail to bind local address:[%s:%d], %s(%d)", 
                 localIp, localPort, strerror(error), error);
        return false;
    }
    return true;
}

int Socket::getProtocolFamily() {
    return addr.getProtocolFamily();
}

}
