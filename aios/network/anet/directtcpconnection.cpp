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
#include "aios/network/anet/directtcpconnection.h"
#include "aios/network/anet/stats.h"
#include "aios/network/anet/log.h"
#include "aios/network/anet/directpacketstreamer.h"
#include "aios/network/anet/directstreamingcontext.h"
#include <assert.h>
#include <errno.h>
#include <stdint.h>

#include "aios/network/anet/channel.h"
#include "aios/network/anet/channelpool.h"
#include "aios/network/anet/connection.h"
#include "aios/network/anet/databuffer.h"
#include "aios/network/anet/directpacket.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/iocomponent.h"
#include "aios/network/anet/ipacketstreamer.h"
#include "aios/network/anet/orderedpacketqueue.h"
#include "aios/network/anet/socket.h"
#include "aios/network/anet/streamingcontext.h"
#include "aios/network/anet/tcpconnection.h"
#include "aios/network/anet/threadcond.h"
#include "aios/network/anet/timeutil.h"

namespace anet {
class IServerAdapter;

constexpr int32_t DIRECT_READ_WRITE_SIZE = READ_WRITE_SIZE;

static const DirectPayload kEmptyPayload{};

DirectTCPConnection::DirectTCPConnection(Socket *socket, IPacketStreamer *streamer,
                                         IServerAdapter *serverAdapter)
    : TCPConnection(socket, streamer, serverAdapter) {
    _directStreamer = dynamic_cast<DirectPacketStreamer *>(streamer);
    assert(nullptr != _directStreamer);
    _directContext = dynamic_cast<DirectStreamingContext *>(_context);
    assert(nullptr != _directContext);
    ANET_LOG(DEBUG, "DirectTCPConnection create");
}

DirectTCPConnection::~DirectTCPConnection() {
    ANET_LOG(DEBUG, "DirectTCPConnection destroy");
    clearWritingPacket();  // in case no error, but packet is still sending
}

bool DirectTCPConnection::writeData() {
    ANET_LOG(DEBUG, "DirectTCPConnection writeData");
    // to reduce the odds of blocking postPacket()
    _outputCond.lock();
    _outputQueue.moveTo(&_myQueue);
    if (_myQueue.size() == 0 && _output.getDataLen() == 0 && _payloadLeftToWrite == 0UL) {
        ANET_LOG(DEBUG, "IOC(%p)->enableWrite(false)", _iocomponent);
        _iocomponent->enableWrite(false);
        _outputCond.unlock();
        return true;
    }
    _outputCond.unlock();
    int ret = 0;
    int writeCnt = 0;
    int myQueueSize = _myQueue.size();
    int error = 0;
    _lasttime = TimeUtil::getTime();
    do {
        while (_output.getDataLen() < DIRECT_READ_WRITE_SIZE) {
            int64_t oldDataLen = _output.getDataLen();
            int64_t oldSpaceAllocated = _output.getSpaceUsed();
            if (_payloadLeftToWrite > 0UL) {
                assert(nullptr != _writingPacket);
                goto CONTINUE_WRITE_PAYLOAD;
            }
            if (myQueueSize == 0) {
                break;
            }
            // _writingPacket must be recycled on error
            _writingPacket = static_cast<anet::DirectPacket *>(_myQueue.pop());
            myQueueSize--;
            _directStreamer->encode(_writingPacket, &_output, _writingPayload);
            if (_writingPayload.getAddr() != nullptr && _writingPayload.getLen() > 0UL) {
                assert(_payloadLeftToWrite == 0UL);
                _payloadLeftToWrite = _writingPayload.getLen();
            }
            {
                int64_t newDataLen = _output.getDataLen();
                int64_t newSpaceAllocated = _output.getSpaceUsed();
                int64_t packetSizeInBuffer = newDataLen - oldDataLen;
                if (packetSizeInBuffer > _maxSendPacketSize) {
                    _maxSendPacketSize = packetSizeInBuffer;
                }
                addOutputBufferSpaceAllocated(newSpaceAllocated - oldSpaceAllocated);
                ANET_ADD_OUTPUT_BUFFER_SPACE_USED(packetSizeInBuffer);
                Channel *channel = _writingPacket->getChannel();
                if (channel) {
                    if (_defaultPacketHandler == NULL && channel->getHandler() == NULL) {
                        // free channel of packets not expecting reply
                        _channelPool.freeChannel(channel);
                    } else {
                        ANET_LOG(SPAM, "channel[%p] expire time[%ld]", channel,
                                 _writingPacket->getExpireTime());
                        channel->setExpireTime(_writingPacket->getExpireTime());
                        _channelPool.addToWaitingList(channel);
                    }
                }
            }
        CONTINUE_WRITE_PAYLOAD:
            if (_payloadLeftToWrite > 0) {  // write direct payload:
                if (_output.getDataLen() > 0) {
                    ret = sendBuffer(writeCnt, error);
                    if (_output.getDataLen() > 0) {
                        break;
                    }
                }
                ret = sendPayload(writeCnt, error);
                if (0UL == _payloadLeftToWrite) {  // write to socket success with payload
                    finishPacketWrite();
                }
                break;
            } else {  // write to buffer success without payload
                finishPacketWrite();
            }
        }
        if (_output.getDataLen() > 0) {
            ret = sendBuffer(writeCnt, error);
        }
    } while (ret > 0 && _output.getDataLen() == 0 && _payloadLeftToWrite == 0 && myQueueSize > 0 &&
             writeCnt < 10);
    _outputCond.lock();
    _outputQueue.moveBack(&_myQueue);
    if (error != 0 && error != EWOULDBLOCK && error != EAGAIN) {
        char spec[32];
        ANET_LOG(WARN, "Connection (%s) write error: %d", _socket->getAddr(spec, 32), error);
        _outputCond.unlock();
        clearOutputBuffer();
        clearWritingPacket();  // clear packet on error
        return false;
    }
    int queueSize = _outputQueue.size() + (_output.getDataLen() > 0 ? 1 : 0) +
                    (_payloadLeftToWrite > 0 ? 1 : 0);
    if (queueSize > 0) {
        // when using level triggered mode, do NOT need to call enableWrite() any more.
    } else if (_writeFinishShutdown) {
        _outputCond.unlock();
        _iocomponent->enableWrite(false);
        _iocomponent->shutdownSocket();
        return true;
    } else if (_writeFinishClose) {
        ANET_LOG(DEBUG, "Initiative cut connect.");
        _outputCond.unlock();
        clearOutputBuffer();
        return false;
    } else {
        ANET_LOG(DEBUG, "IOC(%p)->enableWrite(false)", _iocomponent);
        _iocomponent->enableWrite(false);
    }
    if (!_isServer && _queueLimit > 0) {
        size_t queueTotalSize = _channelPool.getUseListCount();
        if (queueTotalSize < _queueLimit && _waitingThreads > 0) {
            _outputCond.broadcast();
        }
    }
    _outputCond.unlock();
    return true;
}

bool DirectTCPConnection::readData() {
    ANET_LOG(DEBUG, "DirectTCPConnection readData");
    int ret = 0;
    int readCnt = 0;
    bool broken = false;
    int error = 0;

    /** read loop
     * loop until we get a packet anet header, packet size header and message part,
     * then we process them and read payload directly into user given address.
     * broken packet should be handled.
     */
    do {
        ++readCnt;
        int64_t oldSpaceUsed = _input.getSpaceUsed();
        if (!_directContext->isPayloadCompleted()) {
            assert(nullptr != _directContext->getDirectPacket());
            goto CONTINUE_READ_PAYLOAD;
        }
        _input.ensureFree(DIRECT_READ_WRITE_SIZE);
        ret = _socket->read(_input.getFree(), DIRECT_READ_WRITE_SIZE);
        ANET_LOG(SPAM, "%d bytes read", ret);
        if (ret < 0) {
            int64_t newSpaceUsed = _input.getSpaceUsed();
            addInputBufferSpaceAllocated(newSpaceUsed - oldSpaceUsed);
            error = _socket->getSoError();
            break;
        }
        if (0 == ret) {
            _directContext->setEndOfFile(true);
        }
        _input.pourData(ret);
        _stats.totalRxBytes += ret;
        while (_directStreamer->processData(&_input, _directContext)) {
            if (!_directContext->isCompleted()) {
                break;
            }
        CONTINUE_READ_PAYLOAD:
            bool success =
                _directStreamer->readPayload(&_input, _socket, &_channelPool, _directContext, ret);
            if (ret > 0) {
                _stats.totalRxBytes += ret;
            }
            if (!success) {
                _directContext->setPayloadCompleted(false);
                if (!_directContext->isBroken() && ret < 0) {
                    error = _socket->getSoError();
                }
                break;
            }

            // continue original logic
            auto packet = _directContext->stealDirectPacket();
            int64_t packetSizeInBuffer =
                ANET_DIRECT_PACKET_INFO_LEN + packet->getDirectHeader()._msgSize;
            if (packetSizeInBuffer > _maxRecvPacketSize) {
                _maxRecvPacketSize = packetSizeInBuffer;
            }
            assert((int32_t)_directContext->getPayloadReadOffset() ==
                   packet->getDirectHeader()._payloadSize);
            packet->cleanupFlags();
            handlePacket(packet);
            _directContext->reset();
            ANET_COUNT_PACKET_READ(1);
        }
        broken = _directContext->isBroken();

        int64_t newSpaceUsed = _input.getSpaceUsed();
        addInputBufferSpaceAllocated(newSpaceUsed - oldSpaceUsed);
    } while (ret == DIRECT_READ_WRITE_SIZE && !broken && readCnt < 10);

    if (!broken) {
        if (ret == 0) {  // maybe closing connection
            broken = true;
        } else if (ret < 0) {
            if (error != 0 && error != EAGAIN && error != EWOULDBLOCK) {
                char spec[32];
                ANET_LOG(DEBUG, "Connection (%s) read error: %d", _socket->getAddr(spec, 32),
                         error);
                broken = true;
            }
        }
    }
    return !broken;
}

void DirectTCPConnection::clearWritingPacket() {
    assert(_payloadLeftToWrite <= 0UL || nullptr != _writingPacket);
    if (nullptr == _writingPacket) {
        return;
    }
    _writingPacket->free();
    _writingPacket = nullptr;
    _writingPayload = kEmptyPayload;
    _payloadLeftToWrite = 0UL;
}

void DirectTCPConnection::finishPacketWrite() {
    assert(0UL == _payloadLeftToWrite);
    updateQueueStatus(_writingPacket, false);
    _writingPacket->invokeDequeueCB();
    clearWritingPacket();
    ANET_COUNT_PACKET_WRITE(1);
}

int DirectTCPConnection::sendBuffer(int &writeCnt, int &error) {
    int ret = _socket->write(_output.getData(), _output.getDataLen());
    ANET_LOG(SPAM, "%d bytes written from buffer", ret);
    if (ret > 0) {
        _output.drainData(ret);
        _stats.totalTxBytes += ret;
        ANET_ADD_OUTPUT_BUFFER_SPACE_USED(0 - ret);
    } else {
        error = _socket->getSoError();
    }
    writeCnt++;
    return ret;
}

int DirectTCPConnection::sendPayload(int &writeCnt, int &error) {
    assert(_payloadLeftToWrite > 0UL && nullptr != _writingPacket);
    auto addr = _writingPayload.getAddr();
    auto len = _writingPayload.getLen();
    int ret = _socket->write(addr + len - _payloadLeftToWrite, _payloadLeftToWrite);
    if (ret > 0) {
        ANET_LOG(SPAM, "%d bytes written from payload:[%p]", ret, addr);
        _stats.totalTxBytes += ret;
        _payloadLeftToWrite -= ret;
    } else {
        error = _socket->getSoError();
    }
    writeCnt++;
    return ret;
}

}  // namespace anet
