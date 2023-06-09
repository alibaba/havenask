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
#ifndef ANET_CHANNEL_H_
#define ANET_CHANNEL_H_
#include <stdint.h>

#include "aios/network/anet/ipackethandler.h"
#include "aios/network/anet/common.h"
#include "aios/network/anet/directplaceholder.h"

BEGIN_ANET_NS();

class Channel {
    friend class ChannelPool;
    friend class ChannelPoolTest;
    friend class ChannelPoolTest_testAllocChannel_Test;
    friend class ChannelPoolTest_testFreeChannel_Test;
public:
    Channel();
    void init(uint64_t id);
    void setId(uint64_t id);
    uint64_t getId();

    void setArgs(void *args);
    void *getArgs();

    void setHandler(IPacketHandler *handler);
    IPacketHandler *getHandler();

    const DirectPlaceholder &getPlaceholder() const { return _placeholder; }
    void setPlaceholder(const DirectPlaceholder &placeholder) { _placeholder = placeholder; }

    void setExpireTime(int64_t expireTime);
    int64_t getExpireTime() {
        return _expireTime;
    }

    void setTimeoutMs(int32_t timeoutMs);
    int32_t getTimeoutMs() {
        return _timeoutMs;
    }


    Channel *getNext() {
        return _next;
    }
    Channel *getPrev() {
        return _prev;
    }

private:
    uint64_t _id;      // channel id
    void *_args;    // call back argument
    IPacketHandler *_handler;
    DirectPlaceholder _placeholder;
    int64_t _expireTime;
    int32_t _timeoutMs;
private:
    Channel *_prev;
    Channel *_next;
};
END_ANET_NS();

#endif /*CONNECTION_H_*/
