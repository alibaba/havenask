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
/**
 * File name: orderedpacketqueue.h
 * Author: huangrh
 * Create time: 2010-11-22 14:27:14
 * $Id$
 * 
 * Description: ***add description here***
 * 
 */

#ifndef ANET_ORDEREDPACKETQUEUE_H_
#define ANET_ORDEREDPACKETQUEUE_H_
#include "aios/network/anet/common.h"
#include "aios/network/anet/packetqueue.h"

namespace anet {
class Packet;
}  // namespace anet

BEGIN_ANET_NS();
class OrderedPacketQueue : public PacketQueue
{
public:
    OrderedPacketQueue();
    ~OrderedPacketQueue();
public:
    virtual void push(Packet *packet);
    virtual void moveTo(PacketQueue *destQueue);
    virtual void moveBack(PacketQueue *srcQueue);
};

END_ANET_NS();
#endif /*ANET_ORDEREDPACKETQUEUE_H_*/
