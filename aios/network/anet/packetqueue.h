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
#ifndef ANET_PACKET_QUEUE_H_
#define ANET_PACKET_QUEUE_H_
#include "aios/network/anet/common.h"
#include <stddef.h>
#include <stdint.h>

namespace anet {
class Packet;
}  // namespace anet

BEGIN_ANET_NS();
class PacketQueue {
public:
    PacketQueue();
    virtual ~PacketQueue();
public:
    Packet *pop();
    virtual void push(Packet *packet);
    size_t size();
    bool empty();

    /**
     * move all entries of current queue to the tail of destQueue 
     */
    virtual void moveTo(PacketQueue *destQueue);

    /**
     * move all entries of  srcQueue back to the head of current queue
     */
    virtual void moveBack(PacketQueue *srcQueue);

    Packet *getTimeoutList(int64_t now);

protected:
    Packet *_head;
    Packet *_tail;
    size_t _size; 
    friend class OrderedPacketQueue;
};

END_ANET_NS();

#endif

