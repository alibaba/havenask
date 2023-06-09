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
#include "aios/network/anet/log.h"
#include "aios/network/anet/stats.h"
#include "aios/network/anet/streamingcontext.h"
#include "aios/network/anet/channel.h"
#include "aios/network/anet/channelpool.h"
#include "aios/network/anet/socket.h"
#include "aios/network/anet/ipacketstreamer.h"
#include "aios/network/anet/tcpconnection.h"
#include "aios/network/anet/iocomponent.h"
#include <errno.h>
#include <stdint.h>
#include <string.h>

#include "aios/network/anet/connection.h"
#include "aios/network/anet/databuffer.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/orderedpacketqueue.h"
#include "aios/network/anet/packet.h"
#include "aios/network/anet/threadcond.h"
#include "aios/network/anet/timeutil.h"

namespace anet {
class IServerAdapter;

TCPConnection::TCPConnection(Socket *socket, IPacketStreamer *streamer,
                             IServerAdapter *serverAdapter) : Connection(socket, streamer, serverAdapter) {
    _gotHeader = false;
    _writeFinishClose = false;
    _writeFinishShutdown = false;
    memset(&_packetHeader, 0, sizeof(_packetHeader));
    _inputBufferSpaceAllocated = 0;
    _outputBufferSpaceAllocated = 0;
    _maxRecvPacketSize = 0;
    _maxSendPacketSize = 0;
}

TCPConnection::~TCPConnection() {
    addInputBufferSpaceAllocated(0 - _input.getSpaceUsed());
    addOutputBufferSpaceAllocated(0 - _output.getSpaceUsed());
    ANET_ADD_OUTPUT_BUFFER_SPACE_USED(0 - _output.getDataLen());
}

void TCPConnection::clearOutputBuffer() {
    ANET_ADD_OUTPUT_BUFFER_SPACE_USED(0 - _output.getDataLen());
    _output.clear();
}

bool TCPConnection::writeData() {
    //to reduce the odds of blocking postPacket()
    _outputCond.lock();
    _outputQueue.moveTo(&_myQueue);
    if (_myQueue.size() == 0 && _output.getDataLen() == 0) {
        ANET_LOG(DEBUG,"IOC(%p)->enableWrite(false)",_iocomponent);
        _iocomponent->enableWrite(false);
        _outputCond.unlock();
        return true;
    }
    _outputCond.unlock();

    Packet *packet;
    int ret = 0;
    int writeCnt = 0;
    int myQueueSize = _myQueue.size();
    _stats.queueSize = myQueueSize;
    int error = 0;

    _lasttime = TimeUtil::getTime();
    do {
        while (_output.getDataLen() < _readWriteBufSize) {
            if (myQueueSize == 0) {
                break;
            }

            packet = _myQueue.pop();
            myQueueSize --;
            int64_t oldDataLen = _output.getDataLen();
            int64_t oldSpaceAllocated = _output.getSpaceUsed();
            _streamer->encode(packet, &_output);
            int64_t newDataLen = _output.getDataLen();
            int64_t newSpaceAllocated = _output.getSpaceUsed();
            int64_t packetSizeInBuffer = newDataLen - oldDataLen;
            if (packetSizeInBuffer > _maxSendPacketSize) {
                _maxSendPacketSize = packetSizeInBuffer;
            }
            addOutputBufferSpaceAllocated(newSpaceAllocated - oldSpaceAllocated);
            ANET_ADD_OUTPUT_BUFFER_SPACE_USED(packetSizeInBuffer);
            Channel *channel = packet->getChannel();
            if (channel) {
                if (_defaultPacketHandler == NULL
                    && channel->getHandler() == NULL) {
                    //free channle of packets not expecting reply
                    _channelPool.freeChannel(channel);
                } else {
                    ANET_LOG(SPAM, "channel[%p] expire time[%ld]", channel,
                            packet->getExpireTime());
                    channel->setExpireTime(packet->getExpireTime());
                    channel->setTimeoutMs(packet->getTimeoutMs());
                    _channelPool.addToWaitingList(channel);
                }
            }
            updateQueueStatus(packet, false);
            packet->invokeDequeueCB();
            packet->free();

            ANET_COUNT_PACKET_WRITE(1);
        }

        if (_output.getDataLen() == 0) {
            break;
        }

        // write data
        ret = _socket->write(_output.getData(), _output.getDataLen());
        if (ret > 0) {
            _output.drainData(ret);
            _stats.totalTxBytes += ret;
            ANET_ADD_OUTPUT_BUFFER_SPACE_USED(0 - ret);
        } else {
            error = _socket->getSoError();
        }

        writeCnt ++;
    } while (ret > 0 && _output.getDataLen() == 0
             /**@todo remove magic number 10*/
             && myQueueSize>0 && writeCnt < 10);
    _stats.callWriteCount += writeCnt;

    _outputCond.lock();
    _outputQueue.moveBack(&_myQueue);

    if (error !=0 && error != EWOULDBLOCK && error != EAGAIN) {
        char spec[32];
        ANET_LOG(WARN,"Connection (%s) write error: %d",
                 _socket->getAddr(spec, 32), error);
        _outputCond.unlock();
        clearOutputBuffer();
        return false;
    }
    int queueSize = _outputQueue.size() + (_output.getDataLen() > 0 ? 1 : 0);
    if (queueSize > 0) {
//when using level triggered mode, do NOT need to call enableWrite() any more.
//        ANET_LOG(DEBUG,"IOC(%p)->enableWrite(true)", _iocomponent);
//        _iocomponent->enableWrite(true);
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
        ANET_LOG(DEBUG,"IOC(%p)->enableWrite(false)", _iocomponent);
        _iocomponent->enableWrite(false);
    }

    // 如果是client, 并且有queue长度的限制
    if (!_isServer && _queueLimit > 0) {
        size_t queueTotalSize = _channelPool.getUseListCount();
        if (queueTotalSize < _queueLimit && _waitingThreads > 0) {
            _outputCond.broadcast();
        }
    }
    _outputCond.unlock();

    return true;
}

bool TCPConnection::readData() {
    int ret = 0;
    int readCnt = 0;
    bool broken = false;
    int error = 0;

    do {
        readCnt++;
        int64_t oldSpaceUsed = _input.getSpaceUsed();
        _input.ensureFree(_readWriteBufSize);
        ret = _socket->read(_input.getFree(), _readWriteBufSize);
        ANET_LOG(SPAM,"%d bytes read", ret);
        if (ret < 0) {
            int64_t newSpaceUsed = _input.getSpaceUsed();
            addInputBufferSpaceAllocated(newSpaceUsed - oldSpaceUsed);

            error = _socket->getSoError();
            break;
        }
        if (0 == ret) {
            _context->setEndOfFile(true);
        }
        _input.pourData(ret);
        _stats.totalRxBytes += ret;
        while (_streamer->processData(&_input, _context)) {
            if (!_context->isCompleted()) {
                break;
            }
            Packet *packet = _context->stealPacket();
            int64_t packetSizeInBuffer = PACKET_INFO_LEN + packet->getDataLen();
            if (packetSizeInBuffer > _maxRecvPacketSize) {
                _maxRecvPacketSize = packetSizeInBuffer;
            }

            /* Give packet a chance to do some cleanup before passing it to
             * higher level logic. */
            packet->cleanupFlags();

            handlePacket(packet);
            _context->reset();
            ANET_COUNT_PACKET_READ(1);

        }
        broken = _context->isBroken();

        // _streamer->processData() maybe invoke _input.ensureFree()
        int64_t newSpaceUsed = _input.getSpaceUsed();
        addInputBufferSpaceAllocated(newSpaceUsed - oldSpaceUsed);

    } while (ret == _readWriteBufSize && !broken && readCnt < 10);

    _stats.callReadCount += readCnt;

    if (!broken) {
        if (ret == 0) {
            broken = true;
        } else if (ret < 0) {
            if (error != 0 && error != EAGAIN && error != EWOULDBLOCK) {
                char spec[32];
                ANET_LOG(DEBUG,"Connection (%s) read error: %d",
                        _socket->getAddr(spec, 32), error);
                broken = true;
            }
        }
    }

//when using level triggered mode, do NOT need to call enableRead() any more.
//     if (!broken) {
//         _outputCond.lock();
//         ANET_LOG(DEBUG,"IOC(%p)->enableRead(true)", _iocomponent);
//         _iocomponent->enableRead(true);
//         _outputCond.unlock();
//     }

    return !broken;
}

void TCPConnection::addInputBufferSpaceAllocated(int64_t size) {
    if (size == 0) {
        return;
    }
    ANET_ADD_INPUT_BUFFER_SPACE_ALLOCATED(size);
    _inputBufferSpaceAllocated += size;
}

void TCPConnection::addOutputBufferSpaceAllocated(int64_t size) {
    if (size == 0) {
        return;
    }
    ANET_ADD_OUTPUT_BUFFER_SPACE_ALLOCATED(size);
    _outputBufferSpaceAllocated += size;
}

void TCPConnection::shrinkInputBuffer() {
    int64_t oldSpaceUsed = _input.getSpaceUsed();
    _input.shrink();
    int64_t newSpaceUsed = _input.getSpaceUsed();
    addInputBufferSpaceAllocated(newSpaceUsed - oldSpaceUsed);
}

void TCPConnection::shrinkOutputBuffer() {
    int64_t oldSpaceUsed = _output.getSpaceUsed();
    _output.shrink();
    int64_t newSpaceUsed = _output.getSpaceUsed();
    addOutputBufferSpaceAllocated(newSpaceUsed - oldSpaceUsed);
}

}
