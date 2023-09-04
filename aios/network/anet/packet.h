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
#ifndef ANET_PACKET_H_
#define ANET_PACKET_H_
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "aios/network/anet/common.h"
#include "aios/network/anet/connectionpriority.h"

namespace anet {
class Channel;
class DataBuffer;
} // namespace anet

BEGIN_ANET_NS();

#define ANET_PACKET_FLAG 0x416e4574 // AnEt
#define CONNECTION_CLOSE "close"
#define CONNECTION_KEEP_ALIVE "Keep-Alive"

class IPacketHandler;

class PacketHeader {
public:
    uint32_t _chid;   // channel id
    int32_t _pcode;   // packet type
    int32_t _dataLen; // body length
} __attribute__((packed));

static_assert(sizeof(PacketHeader) == 12, "PacketHeader size");

/* ANET_PACKET_FLAG + PacketHeader */
constexpr int PACKET_INFO_LEN = (int)(sizeof(int32_t) + sizeof(PacketHeader));

static_assert(PACKET_INFO_LEN == 16, "PACKET_INFO_LEN size");

class Packet {
    friend class PacketQueue;
    friend class OrderedPacketQueue;
    friend class OrderedPacketQueueTest;
    friend class OrderedPacketQueueTest_testGetTimeoutList_Test;

public:
    Packet();

    virtual ~Packet();

    PacketHeader *getPacketHeader() { return &_packetHeader; }

    void setPacketHeader(PacketHeader *header) {
        if (header) {
            memcpy(&_packetHeader, header, sizeof(PacketHeader));
        }
    }
    /**
     * The Packet is freed through this->free(). In most situation,
     * packet are freed in TCPConnection::writeData() atomatically
     * through this function.
     */
    virtual void free() { delete this; }

    virtual bool isRegularPacket() { return true; }

    /**
     * Write data into DataBuffer. This function is called by
     * Streamer. Streamer uses this function write data into
     * DataBuffer. The packet will be deleted through packet->free()
     * after this function return.
     *
     * @param output target DataBuffer
     * @return Return true when decode success! Else return false.
     */
    virtual bool encode(DataBuffer *output) = 0;

    /**
     * Read data form DataBuffer according to the information in
     * PacketHeader and construct packet. The DataBuffer contains
     * all the data you need. PacketHeader records dataLength and
     * some other information.
     *
     * @param input source data
     * @param header packet information
     * @return Return true when decode success! Else return false.
     */
    virtual bool decode(DataBuffer *input, PacketHeader *header) = 0;

    virtual int64_t getSpaceUsed() { return 0; }

    virtual uint8_t getPacketVersion() { return 0; }

    virtual void setPacketVersion(uint8_t version) {}

    int64_t getExpireTime() { return _expireTime; }
    int32_t getTimeoutMs() { return _timeoutMs; }

    /**
     * Set packet expire time.
     *
     * @param milliseconds. 0: use default. < 0 never timeout.
     */
    void setExpireTime(int milliseconds);

    void setChannel(Channel *channel);

    void setPcode(int32_t pcode);

    int32_t getPcode();

    Channel *getChannel() { return _channel; }

    Packet *getNext() { return _next; }

    IPacketHandler *getDequeueCB(void) { return _packetDequeueCB; }
    void setDequeueCB(IPacketHandler *cb) { _packetDequeueCB = cb; }
    void invokeDequeueCB(void);
    /**
     * The interface to get packet priority for compatibility with advanced
     * packet type. In Packet level, we just assume normal priority for all
     * packets. In the derived advanced packet, we will return the correspond
     * actual priority embeded in the extended header.
     */
    virtual CONNPRIORITY getPriority() { return ANET_PRIORITY_NORMAL; }

    virtual void setPriority(CONNPRIORITY prio) { ; }

    /* Virtual functions to support 64 bit channel id for advance packet. */

    virtual void setChannelId(uint64_t chid) { _packetHeader._chid = (uint32_t)chid; }

    virtual uint64_t getChannelId() { return ((uint64_t)_packetHeader._chid); }

    virtual void setDataLen(int32_t len) { _packetHeader._dataLen = len; }

    virtual int32_t getDataLen(void) { return _packetHeader._dataLen; }

    /* Virtual function to enable advance packet has a chance to cleanup the
     * advance packet flag set in pcode field before passing the packet up to
     * arpc layer. */
    virtual void cleanupFlags() { ; }

    virtual void dump() {
        printf("============Dumping of packet header=============\n");
        printf("chid = 0x%x     pcode = 0x%x    dataLen = %d\n",
               _packetHeader._chid,
               _packetHeader._pcode,
               _packetHeader._dataLen);
    }

protected:
    PacketHeader _packetHeader;
    int64_t _expireTime;
    int32_t _timeoutMs;
    Channel *_channel;

    /* Callback function which will get invoked when packet is moving out
     * of output queue and going to put into the data buffer. The callback
     * should be light weight since it will block the IO thread. The callback
     * is invoked after packet is encoded in output buffer and before packet
     * is freed.
     * So this callback can be used in two scenarios:
     * 1. when app wants to free some data structure when packet is no longer
     *    needed ( the content is serialized to data buffer.)
     * 2. when app wants to sync with the network delivery since as soon as
     *    packet is serialized to databuffer, it will be flushed to network
     *    soon. So this callback is a good way to drive ASIO style
     *    communication
     * Note: Don't do heavy thing in this callback.
     *       Don't delete packet in this callback, it will be freed right after
     *       callback is finished.
     *       User is responsible to keep the callback object before it is used.
     *       The callback will be invoked from the IO thread. */
    IPacketHandler *_packetDequeueCB;
    Packet *_next;
};

END_ANET_NS();

#endif /*PACKET_H_*/
