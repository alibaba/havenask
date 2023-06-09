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
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <list>
#include <sstream>
#include <tr1/unordered_map>
#include <utility>
#include <cstdint>

#include "aios/network/anet/threadmutex.h"
#include "aios/network/anet/channelpool.h"
#include "aios/network/anet/channel.h"
#include "aios/network/anet/atomic.h"
#include "aios/network/anet/common.h"

using namespace std;
BEGIN_ANET_NS();

const uint64_t ChannelPool::AUTO_CHANNEL_ID_MAX = 0x0FFFFFFFFFFFFFFFULL;
const uint64_t ChannelPool::HTTP_CHANNEL_ID = 0x10000000ULL;
const uint64_t ChannelPool::ADMIN_CHANNEL_ID= 0x10000001ULL;

atomic64_t ChannelPool::_globalChannelId = {0};

ChannelPool::ChannelPool() {
    _freeListHead = NULL;
    _freeListTail = NULL;
    _waitReplyHead = NULL;
    _waitReplyTail = NULL;
    atomic_set(&_useCount, 0);
}

ChannelPool::~ChannelPool() {
     std::list<Channel*>::iterator it = _clusterList.begin();

     for (;it != _clusterList.end(); it++) {
         delete[] *it;
     }
}

Channel *ChannelPool::allocChannel(uint64_t chid) {
    MutexGuard freeListGuard(&_freeListLock);
    Channel *channel = NULL;
    if (_freeListHead == NULL) { 
        assert(CHANNEL_CLUSTER_SIZE > 2);
        Channel *channelCluster = new Channel[CHANNEL_CLUSTER_SIZE];
        assert(channelCluster != NULL);
        _clusterList.push_back(channelCluster);
		
        _freeListHead = _freeListTail = &channelCluster[1];
        for (size_t i = 2; i < CHANNEL_CLUSTER_SIZE; i++) {
            _freeListTail->_next = &channelCluster[i];
            channelCluster[i]._prev = _freeListTail;
            _freeListTail = _freeListTail->_next;
        }
        _freeListHead->_prev = NULL;
        _freeListTail->_next = NULL;
        channel = &channelCluster[0];   
    } else {
        channel = _freeListHead;
        _freeListHead = _freeListHead->_next;
        if (_freeListHead != NULL) {
            _freeListHead->_prev = NULL;
        } else {
            _freeListTail = NULL;
        }
    }

    uint64_t id = createChannelId(chid);
    channel->init(id);
    addUseCountByOne();
    return channel;
}

bool ChannelPool::freeChannel(Channel *channel) {
    if (channel == NULL) {
        return false;
    }
    if (channel->getExpireTime() > 0) {
        freeWaitingChannel(channel);
    }
    recycleChannel(channel);
    subUseCountByOne();
    return true;
}

void ChannelPool::freeWaitingChannel(Channel *channel) {
    MutexGuard waitReplyGuard(&_waitReplyLock);
    _waitReplyMap.erase(channel->_id);
    if (channel == _waitReplyHead) {
        _waitReplyHead = channel->_next;
    }
    if (channel == _waitReplyTail) {
        _waitReplyTail = channel->_prev;
    }
    if (channel->_prev != NULL) {
        channel->_prev->_next = channel->_next;
    }
    if (channel->_next != NULL) {
        channel->_next->_prev = channel->_prev;
    }
}

void ChannelPool::recycleChannel(Channel *channel) {
    MutexGuard freeListGuard(&_freeListLock);
    channel->_prev = _freeListTail;
    channel->_next = NULL;
    if (_freeListTail == NULL) {
        _freeListHead = channel;
    } else {
        _freeListTail->_next = channel;
    }
    _freeListTail = channel;
}

Channel *ChannelPool::findChannel(uint64_t id) {
    Channel *channel = NULL;
    std::tr1::unordered_map<uint64_t, Channel*>::iterator it;
    MutexGuard waitReplyGuard(&_waitReplyLock);
    it = _waitReplyMap.find(id);
    if (it != _waitReplyMap.end()) {
        channel = it->second;
    }
    return channel;
}

Channel* ChannelPool::getTimeoutList(int64_t now) {
    MutexGuard waitReplyGuard(&_waitReplyLock);
    Channel *list = NULL;
    if (_waitReplyHead == NULL) {
        return list;
    }

    Channel *channel = _waitReplyHead;
    while (channel != NULL) {
        if (channel->_expireTime == 0 || channel->_expireTime >= now) {
            break;
        }
        _waitReplyMap.erase(channel->_id);
        subUseCountByOne();
        channel = channel->_next;
    }

    if (channel != _waitReplyHead) {
        list = _waitReplyHead;
        if (channel == NULL) { 
            _waitReplyHead = _waitReplyTail = NULL;
        } else {
            channel->_prev->_next = NULL;
            channel->_prev = NULL;
            _waitReplyHead = channel;
        }
    }
    return list;
}

bool ChannelPool::appendFreeList(Channel *addList) {
    MutexGuard freeListGuard(&_freeListLock);
    if (addList == NULL) {
        return true;
    }
    
    Channel *tail = addList;
    while (tail->_next != NULL) {
        tail = tail->_next;
    }

    addList->_prev = _freeListTail;
    if (_freeListTail == NULL) {
        _freeListHead = addList;
    } else {
        _freeListTail->_next = addList;
    }
    _freeListTail = tail;

    return true;
}

void ChannelPool::addToWaitingList(Channel *channel) {
    if (channel->getExpireTime() == 0) {
        return;
    }
    MutexGuard waitReplyGuard(&_waitReplyLock);
    _waitReplyMap[channel->getId()] = channel;
    if (_waitReplyHead == NULL) {
        // empty wait list
        channel->_prev = channel->_next = NULL;
        _waitReplyHead = _waitReplyTail = channel;
        return;
    }
    if (_waitReplyHead->getExpireTime() > channel->getExpireTime()) {
        // channel's expire time less than head of the list
        channel->_prev = NULL;
        channel->_next = _waitReplyHead;
        _waitReplyHead->_prev = channel;
        _waitReplyHead = channel;
    } else {
        Channel *nextChannel = _waitReplyHead->_next;
        while (nextChannel) {
            if (nextChannel->getExpireTime() > channel->getExpireTime()) {
                break;
            }
            nextChannel = nextChannel->_next;
        }
        if (nextChannel == NULL) {
            // channel's expire time larger than all channel in the list
            channel->_prev = _waitReplyTail;
            channel->_next = NULL;
            _waitReplyTail->_next = channel;
            _waitReplyTail = channel;
        } else {
            Channel *preChannel = nextChannel->_prev;
            channel->_prev = preChannel;
            channel->_next = nextChannel;            
            preChannel->_next = channel;
            nextChannel->_prev = channel;
        }
    }
}

uint64_t ChannelPool::createChannelId(uint64_t chid) {
    uint64_t id = chid;
    if (id <= AUTO_CHANNEL_ID_MAX && (id != HTTP_CHANNEL_ID &&
        id != ADMIN_CHANNEL_ID)) {
        do {
            id = atomic_add_return(1, &_globalChannelId);
            id &= AUTO_CHANNEL_ID_MAX;
            /* Tricky. With 64 bit channel id, we just skip the original
             * HTTP channel ID and admin channel id. */
            if ((uint32_t)id == HTTP_CHANNEL_ID || 
                (uint32_t)id == ADMIN_CHANNEL_ID || 
                (uint32_t)id == 0)
            {
                id = 0;
                continue; 
            }
        } while (0 == id);
    }
    return id;
}

END_ANET_NS();


