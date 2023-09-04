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
#include "aios/network/anet/channel.h"

#include <stdint.h>
#include <stdlib.h>

namespace anet {
class IPacketHandler;

Channel::Channel() { init(0); }

void Channel::init(uint64_t id) {
    _prev = NULL;
    _next = NULL;
    _id = id;
    _expireTime = 0;
    _timeoutMs = 0;
    _handler = NULL;
    _args = NULL;
}

void Channel::setId(uint64_t id) { _id = id; }

uint64_t Channel::getId() { return _id; }

void Channel::setArgs(void *args) { _args = args; }

void *Channel::getArgs() { return _args; }

void Channel::setHandler(IPacketHandler *handler) { _handler = handler; }

IPacketHandler *Channel::getHandler() { return _handler; }

/*
 * set expirt time
 *
 * @param milliseconds, 0 never timeout
 */
void Channel::setExpireTime(int64_t expireTime) { _expireTime = expireTime; }

/*
 * set raw expirt time
 *
 */
void Channel::setTimeoutMs(int32_t timeoutMs) { _timeoutMs = timeoutMs; }

} // namespace anet
