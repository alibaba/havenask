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
#include "aios/network/anet/packetqueue.h"
#include "aios/network/anet/packet.h"

#include "aios/network/anet/common.h"

BEGIN_ANET_NS();

PacketQueue::PacketQueue() {
    _head = NULL;
    _tail = NULL;
    _size = 0;
}

PacketQueue::~PacketQueue() {
    Packet* p = _head;
    while(p) {
        Packet* q = p;
        p = p->_next;
        q->free();
    }
    _head = NULL;
    _tail = NULL;
    _size = 0;
}
    
Packet *PacketQueue::pop() {
    if (_head == NULL) {
        return NULL;
    }
    Packet *packet = _head;
    _head = _head->_next;
    if (_head == NULL) {
        _tail = NULL;
    }
    _size --;
    return packet;
}

void PacketQueue::push(Packet *packet) {
    packet->_next = NULL;
    if (_tail == NULL) {
        _head = packet;
    } else {
        _tail->_next = packet;
    }
    _tail = packet;
    _size++;
}
  
size_t PacketQueue::size() {
    return _size;
}

bool PacketQueue::empty() {
    return (_size == 0);
}


void PacketQueue::moveTo(PacketQueue *destQueue) {
    if (_head == NULL) { 
        return;
    }
    if (destQueue->_tail == NULL) {
        destQueue->_head = _head;
    } else {
        destQueue->_tail->_next = _head;
    }
    destQueue->_tail = _tail;
    destQueue->_size += _size;
    _head = _tail = NULL;
    _size = 0;
}

void PacketQueue::moveBack(PacketQueue *srcQueue) {
    if (NULL == srcQueue->_head) {
        return;
    }
    if (NULL == _tail) {
        _head = srcQueue->_head;
    } else {
        _tail->_next = srcQueue->_head;
    }
    _tail = srcQueue->_tail;
    _size += srcQueue->_size;
    srcQueue->_head = srcQueue->_tail = NULL;
    srcQueue->_size = 0;
}

Packet *PacketQueue::getTimeoutList(int64_t now)
{
    Packet *list, *tail;
    list = tail = NULL;
    while (_head != NULL) {
        int64_t t = _head->getExpireTime();
        if (t >= now) {
            break;
        }
        if (tail == NULL) {
            list = _head;
        } else {
            tail->_next = _head;
        }
        tail = _head;
        _size --;
        _head = _head->_next;
    }
    if (tail) {
        tail->_next = NULL;
    }
    
    if (!_head) {
        _tail = _head;
    }

    return list;
}
END_ANET_NS();
