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
#include "aios/network/anet/connection.h"

#include <assert.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>

#include "aios/network/anet/atomic.h"
#include "aios/network/anet/channel.h"
#include "aios/network/anet/channelpool.h"
#include "aios/network/anet/common.h"
#include "aios/network/anet/connectionpriority.h"
#include "aios/network/anet/controlpacket.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/iocomponent.h"
#include "aios/network/anet/ipackethandler.h"
#include "aios/network/anet/ipacketstreamer.h"
#include "aios/network/anet/iserveradapter.h"
#include "aios/network/anet/log.h"
#include "aios/network/anet/orderedpacketqueue.h"
#include "aios/network/anet/packet.h"
#include "aios/network/anet/socket.h"
#include "aios/network/anet/stats.h"
#include "aios/network/anet/streamingcontext.h"
#include "aios/network/anet/threadcond.h"
#include "aios/network/anet/threadmutex.h"
#include "aios/network/anet/timeutil.h"
#include "autil/EnvUtil.h"

struct sockaddr_in;

BEGIN_ANET_NS();

Connection::Connection(Socket *socket, IPacketStreamer *streamer, IServerAdapter *serverAdapter) {
    assert(streamer);
    _isServer = false;
    _socket = socket;
    _streamer = streamer;
    _serverAdapter = serverAdapter;
    _defaultPacketHandler = NULL;
    _iocomponent = NULL;
    _queueTimeout = 5000;
    _waitingThreads = 0;
    _closed = false;
    _queueLimit = _streamer->existPacketHeader() ? 50 : 1;
    _context = _streamer->createContext();
    atomic_set(&_outputQueueSize, 0);
    atomic_set(&_outputQueueSpaceUsed, 0);
    _rwCbState = RW_CB_IDLE;
    _socketAddr = NULL;
    _socketAddrLen = 0;
    _jobId = 0;
    _insId = 0;
    _qosId = 0;
    _lasttime = TimeUtil::getTime();
    _readWriteBufSize = autil::EnvUtil::getEnv("ANET_READ_WRITE_SIZE", _readWriteBufSize);
}

Connection::~Connection() {
    if (!isClosed()) {
        closeHook();
    }
    delete _context;
    delete[] _socketAddr;
    _socketAddrLen = 0;
}

void Connection::close() { _iocomponent->closeConnection(this); }

bool Connection::isClosed() { return _closed; }

bool Connection::isConnected() {
    if (!_iocomponent) {
        return false;
    }
    return _iocomponent->getState() == IOComponent::ANET_CONNECTED;
}

bool Connection::postPacket(Packet *packet, IPacketHandler *packetHandler, void *args, bool block) {

    ANET_LOG(SPAM, "postPacket() through IOC(%p).", _iocomponent);
    if (NULL == packet) {
        ANET_LOG(WARN, "packet is NULL, post failed.");
        return false;
    }

    if (packet->getExpireTime() == 0) {
        packet->setExpireTime(_queueTimeout);
    }
    _outputCond.lock();
    if (isClosed()) {
        ANET_LOG(WARN, "Connection closed! IOC(%p).", _iocomponent);
        _outputCond.unlock();
        return false;
    }
    if (!_isServer) {
        int64_t now = TimeUtil::getTime();
        if (_outputQueue.size() == 0) {
            _lasttime = now;
        } else if (now > _lasttime + 4000000) { // 4s
            char remoteAddr[32] = {0};
            _socket->getRemoteAddr(remoteAddr, 32);
            remoteAddr[31] = 0;
            ANET_LOG(
                WARN, "disconnect: %s, size:%ld, now: %ld, last: %ld", remoteAddr, _outputQueue.size(), now, _lasttime);
            _outputCond.unlock();
            return false;
        }
    }
    if (!_isServer && _queueLimit > 0) { //@fix bug #97
        if (!block && _channelPool.getUseListCount() >= _queueLimit) {
            _outputCond.unlock();
            char remoteAddr[32] = {0};
            _socket->getRemoteAddr(remoteAddr, 32);
            remoteAddr[31] = 0;
            ANET_LOG(WARN,
                     "QUEUE FULL NO BLOCK! IOC(%p), QueueLimit(%d), RemoteAddr(%s) .",
                     _iocomponent,
                     (int)_queueLimit,
                     remoteAddr);
            return false; // will not block
        }
        while (_channelPool.getUseListCount() >= _queueLimit && !isClosed()) {
            _waitingThreads++;
            ANET_LOG(SPAM, "Before Waiting! IOC(%p).", _iocomponent);
            _outputCond.wait();
            ANET_LOG(SPAM, "After Waiting! IOC(%p).", _iocomponent);
            _waitingThreads--;
        }
        if (isClosed()) {
            ANET_LOG(WARN, "Waked up on closing! IOC(%p).", _iocomponent);
            _outputCond.unlock();
            return false;
        }
    }

    if (_isServer) {
        if (_streamer->existPacketHeader()) {
            assert(packet->getChannelId());
        }
    } else {
        uint64_t chid = _streamer->existPacketHeader() ? 0 : ChannelPool::HTTP_CHANNEL_ID;
        Channel *channel = _channelPool.allocChannel(chid);
        if (NULL == channel) {
            ANET_LOG(WARN, "Failed to allocate a channel");
            _outputCond.unlock();
            return false;
        }
        channel->setHandler(packetHandler);
        channel->setArgs(args);
        packet->setChannel(channel);
    }

    // write to outputqueue
    int qsize = _outputQueue.size();
    _outputQueue.push(packet);
    updateQueueStatus(packet, true);
    if (qsize == 0) {
        ANET_LOG(DEBUG, "IOC(%p)->enableWrite(true)", _iocomponent);
        _iocomponent->enableWrite(true);
    }
    _stats.packetPosted++;

    /* We should do the logging before lock is released to prevent
     * racing of packet object. */
    ANET_LOG(DEBUG, "postPacket() Success! IOC(%p), chid %lu.", _iocomponent, packet->getChannelId());

    _outputCond.unlock();
    return true;
}

class SynStub : public IPacketHandler {
public:
    HPRetCode handlePacket(Packet *packet, void *args) {
        MutexGuard guard(&_cond);
        assert(this == args);
        _reply = packet;
        _cond.signal();
        return FREE_CHANNEL;
    }

    Packet *waitReply() {
        _cond.lock();
        while (NULL == _reply) {
            _cond.wait();
        }
        _cond.unlock();
        return _reply;
    }

    SynStub() { _reply = NULL; }
    virtual ~SynStub() {}

private:
    ThreadCond _cond;
    Packet *_reply;
};

Packet *Connection::sendPacket(Packet *packet, bool block) {
    SynStub stub;
    if (_isServer || !postPacket(packet, &stub, &stub, block)) {
        return NULL;
    }
    return stub.waitReply();
}

bool Connection::handlePacket(Packet *packet) {
    void *args = NULL;
    Channel *channel = NULL;
    IPacketHandler *packetHandler = NULL;

    if (_isServer) {
        _rwCbState = RW_CB_RUNNING;
        beforeCallback();
        _serverAdapter->handlePacket(this, packet);
        afterCallback();
        _rwCbState = RW_CB_IDLE;
        return true;
    }

    if (!_streamer->existPacketHeader()) {
        packet->setChannelId(ChannelPool::HTTP_CHANNEL_ID);
    }

    uint64_t chid = packet->getChannelId();
    channel = _channelPool.findChannel(chid);
    if (channel == NULL /*|| channel->getExpireTime() == 0*/) {
        // trick! to filter out channle for packet still in _outputQueue
        ANET_LOG(WARN, "No channel found, id: %llu", (unsigned long long)chid);
        packet->free();
        return false;
    }
    packetHandler = channel->getHandler();
    args = channel->getArgs();
    _channelPool.freeChannel(channel); // fix bug 137
    if (packetHandler == NULL) {
        packetHandler = _defaultPacketHandler;
    }
    assert(packetHandler);
    _rwCbState = RW_CB_RUNNING;
    beforeCallback();
    packetHandler->handlePacket(packet, args);
    afterCallback();
    _rwCbState = RW_CB_IDLE;
    wakeUpSender();
    return true;
}

bool Connection::checkTimeout(int64_t now) {
    ANET_LOG(SPAM, "check Timeout  IOC(%p), CONN(%p), now[%ld]", _iocomponent, this, now);
    Channel *list = NULL;
    Channel *channel = NULL;
    IPacketHandler *packetHandler = NULL;
    Packet *cmdPacket = (now == TimeUtil::MAX) ? &ControlPacket::ConnectionClosedPacket : &ControlPacket::TimeoutPacket;

    if (!_isServer && (list = _channelPool.getTimeoutList(now))) {
        ANET_LOG(SPAM, "Checking channel pool. IOC(%p), CONN(%p)", _iocomponent, this);
        channel = list;
        if (!_streamer->existPacketHeader() && cmdPacket == &ControlPacket::TimeoutPacket) {
            _iocomponent->closeAndSetState();
        }
        while (channel != NULL) {
            char buff[32] = {0};
            _socket->getRemoteAddr(buff, 32);
            buff[31] = 0;
            ANET_LOG(WARN,
                     "Channel timeout (%d ms)! id: %llu, remoteAddr: %s. IOC(%p), CONN(%p).",
                     channel->getTimeoutMs(),
                     (unsigned long long)channel->getId(),
                     buff,
                     _iocomponent,
                     this);
            packetHandler = channel->getHandler();
            if (packetHandler == NULL) {
                packetHandler = _defaultPacketHandler;
            }
            if (packetHandler != NULL) {
                beforeCallback();
                packetHandler->handlePacket(cmdPacket, channel->getArgs());
                afterCallback();
            }
            _stats.packetTimeout++;
            ANET_COUNT_PACKET_TIMEOUT(1);
            channel = channel->getNext();
        }
        _channelPool.appendFreeList(list);
    }

    _outputCond.lock();
    ANET_LOG(SPAM, "Checking output queue(%lu). IOC(%p), CONN(%p).", _outputQueue.size(), _iocomponent, this);
    Packet *packetList = _outputQueue.getTimeoutList(now);
    _outputCond.unlock();
    while (packetList) {
        char buff[32] = {0};
        _socket->getRemoteAddr(buff, 32);
        buff[31] = 0;
        ANET_LOG(WARN,
                 "Packet Timeout. Now(%ld), Exp(%ld), queue timeout(%d), timeout(%d), remote address(%s). IOC(%p), "
                 "CONN(%p).",
                 now,
                 packetList->getExpireTime(),
                 _queueTimeout,
                 packetList->getTimeoutMs(),
                 buff,
                 _iocomponent,
                 this);
        Packet *packet = packetList;
        packetList = packetList->getNext();

        /* need to update connection counter earlier since handlePacket will
         * trigger ARPC closure->Run() and thus free the msg, which will
         * cause crash when we try to calculate the msg size through SpaceUsed
         */
        updateQueueStatus(packet, false);
        if (_isServer) {
            if (_serverAdapter) {
                beforeCallback();
                _serverAdapter->handlePacket(this, cmdPacket);
                afterCallback();
            }
        } else {
            channel = packet->getChannel();
            if (channel) {
                packetHandler = channel->getHandler();
                if (packetHandler == NULL) {
                    packetHandler = _defaultPacketHandler;
                }
                void *args = channel->getArgs();
                _channelPool.freeChannel(channel); //~Bug #93
                if (packetHandler != NULL) {
                    beforeCallback();
                    packetHandler->handlePacket(cmdPacket, args);
                    afterCallback();
                }
            }
        }
        _stats.packetTimeout++;
        ANET_COUNT_PACKET_TIMEOUT(1);
        packet->free();
    }

    wakeUpSender();
    return true;
}

void Connection::closeHook() {
    _outputCond.lock();
    bool closed = _closed;
    _closed = true;
    _outputCond.unlock();
    if (!closed) { // the first time to call closeHook
        if (_isServer && _serverAdapter) {
            beforeCallback();
            _serverAdapter->handlePacket(this, &ControlPacket::CloseConnectionPacket);
            afterCallback();
        }
    }
    checkTimeout(TimeUtil::MAX);
}

void Connection::openHook() {
    _outputCond.lock();
    _closed = false;
    _outputCond.unlock();
}

void Connection::wakeUpSender() {
    if (_queueLimit > 0) {
        _outputCond.lock();
        size_t queueTotalSize = _channelPool.getUseListCount();
        if (queueTotalSize < _queueLimit || isClosed()) {
            if (_waitingThreads) {
                _outputCond.broadcast();
            }
        }
        _outputCond.unlock();
    }
}

void Connection::beforeCallback() { _iocomponent->unlock(); }

void Connection::afterCallback() {
    _iocomponent->lock();
    _stats.packetHandled++;
}

void Connection::addRef() { _iocomponent->addRef(); }

void Connection::subRef() { _iocomponent->subRef(); }

int Connection::getRef() { return _iocomponent->getRef(); }

bool Connection::setQueueLimit(size_t limit) {
    if (_streamer->existPacketHeader()) {
        _queueLimit = limit;
        return true;
    }
    if (limit != 1) {
        return false;
    } else {
        return true;
    }
}

size_t Connection::getQueueLimit() { return _queueLimit; }

void Connection::resetContext() {
    clearOutputBuffer();
    clearInputBuffer();
    _context->reset();
}

void Connection::updateQueueStatus(Packet *packet, bool pushOrPopFlag) {
    int64_t spaceUsed = 0;
    if (pushOrPopFlag) {
        atomic_inc(&_outputQueueSize);
        spaceUsed = packet->getSpaceUsed();
        ANET_ADD_OUTPUT_QUEUE_SIZE(1);
    } else {
        atomic_dec(&_outputQueueSize);
        spaceUsed = 0 - packet->getSpaceUsed();
        ANET_ADD_OUTPUT_QUEUE_SIZE(-1);
    }
    atomic_add(spaceUsed, &_outputQueueSpaceUsed);
    ANET_ADD_OUTPUT_QUEUE_SPACE_USED(spaceUsed);
}

int Connection::setPriority(CONNPRIORITY priority) { return _socket->setPriority(priority); }

int Connection::setQosGroup(uint64_t jobid, uint32_t instanceid, uint32_t groupid) {
    _jobId = jobid;
    _insId = instanceid;
    _qosId = groupid;
    if (_qosId == 0)
        return 0;
    if (_iocomponent->getState() == IOComponent::ANET_CONNECTED) {
        int ret = _socket->setQosGroup(_jobId, _insId, _qosId);
        ANET_ALOG_INFO(logger,
                       "socket %d _jobId=%lu, _insId=%u, _qosId=%u ret=%d",
                       _socket->getSocketHandle(),
                       _jobId,
                       _insId,
                       _qosId,
                       ret);
        return ret;
    }
    return 0;
}
int Connection::setQosGroup() {
    int ret = 0;
    if (_qosId == 0)
        return 0;
    ret = _socket->setQosGroup(_jobId, _insId, _qosId);
    ANET_ALOG_INFO(logger,
                   "socket %d _jobId=%lu, _insId=%u, _qosId=%u ret=%d",
                   _socket->getSocketHandle(),
                   _jobId,
                   _insId,
                   _qosId,
                   ret);
    return ret;
}
uint32_t Connection::getQosGroup() { return _qosId; }
CONNPRIORITY Connection::getPriority() { return (CONNPRIORITY)_socket->getPriority(); }

RECONNErr Connection::reconnect() { return _iocomponent->reconnect(); }

char *Connection::getLocalAddr(char *dest, int len) { return _socket->getAddr(dest, len, true); }

char *Connection::getIpAndPortAddr(char *dest, int len) {
    if (_socketAddr == NULL) {
        _addrMutex.lock();
        if (_socketAddr == NULL) {
            if (_socket->getProtocolFamily() == AF_UNIX) {
                _addrMutex.unlock();
                return NULL;
            }
            char *buff = new char[32];
            if (_socket->getAddr(buff, 32) == NULL) {
                _addrMutex.unlock();
                delete[] buff;
                return NULL;
            }
            _socketAddrLen = strlen(buff);
            _socketAddr = buff;
        }
        _addrMutex.unlock();
    }
    int dataLen = len > (int)_socketAddrLen ? (int)_socketAddrLen : len;
    memcpy(dest, _socketAddr, dataLen);
    if (dataLen < len)
        dest[dataLen] = '\0';
    else
        dest[len - 1] = '\0';
    return dest;
}

bool Connection::getSockAddr(sockaddr_in &addr) { return _socket->getSockAddr(addr); }

bool Connection::setKeepAliveParameter(int idleTime, int keepInterval, int cnt) {
    return _socket->setKeepAliveParameter(idleTime, keepInterval, cnt);
}

ConnStat Connection::getConnStat() {
    if (_stats.localAddr.empty()) {
        char addr[256];
        if (getLocalAddr(addr, 256)) {
            _stats.localAddr = addr;
        }
    }
    if (_stats.peerAddr.empty()) {
        char addr[256];
        if (getIpAndPortAddr(addr, 256)) {
            _stats.peerAddr = addr;
        }
    }
    return _stats;
}

END_ANET_NS();
