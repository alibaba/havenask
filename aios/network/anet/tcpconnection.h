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
#ifndef ANET_TCPCONNECTION_H_
#define ANET_TCPCONNECTION_H_
#include <stdint.h>

#include "aios/network/anet/connection.h"
#include "aios/network/anet/databuffer.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/iocomponent.h"
#include "aios/network/anet/log.h"
#include "aios/network/anet/packet.h"
#include "aios/network/anet/socket.h"

namespace anet {

class DataBuffer;
class Socket;
class IPacketStreamer;
class IServerAdapter;

class TCPConnection : public Connection {
    friend class TCPConnectionTest_testWriteData_Test;

public:
    TCPConnection(Socket *socket, IPacketStreamer *streamer, IServerAdapter *serverAdapter);

    ~TCPConnection();

    /*
     * 写出数据
     *
     * @return 是否成功
     */
    bool writeData();

    /*
     * 读入数据
     *
     * @return 读入数据
     */
    bool readData();

    /*
     * 设置写完是否主动关闭
     */
    void setWriteFinishClose(bool v) { _writeFinishClose = v; }
    void setWriteFinishShutdown(bool v) { _writeFinishShutdown = v; }

    /*
     * 清空output的buffer
     */
    void clearOutputBuffer();

    /*
     * 清空output的buffer
     */
    void clearInputBuffer() { _input.clear(); }

    void addInputBufferSpaceAllocated(int64_t size);
    void addOutputBufferSpaceAllocated(int64_t size);
    int64_t getInputBufferSpaceAllocated() { return _inputBufferSpaceAllocated; };
    int64_t getOutputBufferSpaceAllocated() { return _outputBufferSpaceAllocated; };

    void shrinkInputBuffer();
    void shrinkOutputBuffer();

    int64_t getMaxRecvPacketSize() { return _maxRecvPacketSize; }
    int64_t getMaxSendPacketSize() { return _maxSendPacketSize; }

    void clearMaxRecvPacketSize() { _maxRecvPacketSize = 0; }
    void clearMaxSendPacketSize() { _maxSendPacketSize = 0; }
    int setQosGroup(uint64_t jobid, uint32_t instanceid, uint32_t groupid) {
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
    int setQosGroup() {
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

protected:
    DataBuffer _output;         // 输出的buffer
    DataBuffer _input;          // 读入的buffer
    PacketHeader _packetHeader; // 读入的packet header
    bool _gotHeader;            // packet header已经取过
    bool _writeFinishClose;     // 写完断开
    bool _writeFinishShutdown;  // 写完shutdown
    int64_t _inputBufferSpaceAllocated;
    int64_t _outputBufferSpaceAllocated;

    /*
     * These two variables are used to track the max recv/send packet size,
     * and will be reset to 0 at every shrink interval in timeout thread.
     */
    int64_t _maxRecvPacketSize;
    int64_t _maxSendPacketSize;
};

} // namespace anet

#endif /*TCPCONNECTION_H_*/
