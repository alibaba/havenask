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
 * File name: orderedpacketqueue.cpp
 * Author: huangrh
 * Create time: 2010-11-22 14:27:14
 * $Id$
 * 
 * Description: ***add description here***
 * 
 */

#include "aios/network/anet/orderedpacketqueue.h"
#include <stddef.h>
#include <stdint.h>

#include "aios/network/anet/common.h"
#include "aios/network/anet/packet.h"
#include "aios/network/anet/packetqueue.h"

BEGIN_ANET_NS();
OrderedPacketQueue::OrderedPacketQueue() {
}

OrderedPacketQueue::~OrderedPacketQueue() {
}

void OrderedPacketQueue::push(Packet *packet) {
    packet->_next = NULL;
    _size++;

    if (_head == NULL) {
        _head = packet;
        _tail = packet;
        return;
    }

    int64_t expireTime = packet->getExpireTime();

    if (expireTime >= _tail->getExpireTime()) {
        _tail->_next = packet;
        _tail = packet;
        return;
    }

    if (expireTime < _head->getExpireTime()) {
        packet->_next = _head;
        _head = packet;
        return;
    }
    
    Packet *p1 = _head;
    Packet *p2 = p1;
    while(expireTime >= p1->getExpireTime()) {
        p2 = p1;
        p1 = p1->_next;
    }
    packet->_next = p1;
    p2->_next = packet;
}

void OrderedPacketQueue::moveTo(PacketQueue *destQueue) {
    destQueue->moveBack(this);
}

void OrderedPacketQueue::moveBack(PacketQueue *srcQueue) {
    if (NULL == srcQueue->_head) {
        return;
    }

    _size += srcQueue->_size;
    srcQueue->_size = 0;

    if (NULL == _head) {
        _head = srcQueue->_head;
        _tail = srcQueue->_tail;
        srcQueue->_head = srcQueue->_tail = NULL;
        return;
    }

    if(srcQueue->_tail->getExpireTime() <= _head->getExpireTime()) {
        srcQueue->_tail->_next = _head;
        _head = srcQueue->_head;
        srcQueue->_head = srcQueue->_tail = NULL;
        return;
    } 

    Packet *head = _head;
    Packet *lIter = _head;
    Packet *rIter= srcQueue->_head;
    if (lIter->getExpireTime() < rIter->getExpireTime()) {
        head = lIter;
        lIter = lIter->_next;
    } else {
        head = rIter;
        rIter= rIter->_next;
    }

    Packet *currIter = head;
    while (lIter && rIter) {
        if (lIter->getExpireTime() < rIter->getExpireTime()) {
            currIter->_next = lIter;
            lIter = lIter->_next;
        } else {
            currIter->_next = rIter;
            rIter= rIter->_next;
        }
        currIter = currIter->_next;
    }
    if (lIter) {
        currIter->_next = lIter;
    } else {
        currIter->_next = rIter;
        _tail = srcQueue->_tail;
    }
    _head = head;
    srcQueue->_head = srcQueue->_tail = NULL;
}

END_ANET_NS();
