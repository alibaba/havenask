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
#ifndef ANET_CHANNEL_POOL_H_
#define ANET_CHANNEL_POOL_H_
#include <stdint.h>
#include <sys/types.h>
#include <stdlib.h>
#include <list>
#include <tr1/unordered_map>

#include "aios/network/anet/atomic.h"
#include "aios/network/anet/threadmutex.h"
#include "aios/network/anet/channel.h"
#include "aios/network/anet/common.h"

namespace anet {
const size_t CHANNEL_CLUSTER_SIZE = 64lu;

class ChannelPool {
public:
    static const uint64_t AUTO_CHANNEL_ID_MAX;
    static const uint64_t HTTP_CHANNEL_ID;
    static const uint64_t ADMIN_CHANNEL_ID;
public:
    ChannelPool();
    ~ChannelPool();

public:
    /**
     * Allocate a new channel.
     *
     * @param chid The desired channel id for the channel just allocated. If
     * chid is less than or equal to AUTO_CHANNEL_ID_MAX, the channel id of
     * the new channel will be generated automatically, which is larger than
     * 0 and less than AUTO_CHANNEL_ID_MAX. if chid is greater than
     * AUTO_CHANNEL_ID_MAX,  the channel id of the new channel will be chid.
     */
    Channel* allocChannel(uint64_t chid = 0);
    bool freeChannel(Channel *channel);
    Channel* findChannel(uint64_t id);
    Channel* getTimeoutList(int64_t now);
    bool appendFreeList(Channel *addList);
    void addToWaitingList(Channel *channel);

    size_t getUseListCount() {
        return atomic_read(&_useCount);
    }

private:
    inline void addUseCountByOne()     {
        atomic_add(1, &_useCount);
    }

    inline void subUseCountByOne()     {
        atomic_sub(1,&_useCount);
    }


    uint64_t createChannelId(uint64_t chid);
    void freeWaitingChannel(Channel *channel);
    void recycleChannel(Channel *channel);

private:
    std::tr1::unordered_map<uint64_t, Channel*> _waitReplyMap;
    std::list<Channel*> _clusterList;

    ThreadMutex _freeListLock;
    ThreadMutex _waitReplyLock;

    Channel *_freeListHead;
    Channel *_freeListTail;

    Channel *_waitReplyHead;
    Channel *_waitReplyTail;
    atomic_t _useCount;

    static atomic64_t _globalChannelId;

    friend class ChannelPoolTest;
    friend class ChannelPoolTest_testAllocateWhenExceedMax_Test;
    friend class ChannelPoolTest_testAllocChannel_Test;
    friend class ChannelPoolTest_testFreeChannelEmpty_Test;
    friend class ChannelPoolTest_testFreeChannelOne_Test;
    friend class ChannelPoolTest_testFreeChannelNotInWaitlist_Test;
    friend class ChannelPoolTest_testFreeChannelInWaitlist_Test;
    friend class ChannelPoolTest_testFreeChannelComplex_Test;
    friend class ChannelPoolTest_testFreeChannel_Test;
    friend class ChannelPoolTest_testFreeChannelMany_Test;
    friend class ChannelPoolTest_testAppendFreeList_Test;
    friend class ChannelPoolTest_testCreateChannelId_Test;
    friend class ConnectionTest_testBugfix_211465_Test;
};

}

#endif /*CHANNEL_POOL_H_*/
