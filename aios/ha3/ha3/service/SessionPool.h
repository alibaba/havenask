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
#pragma once

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <memory>
#include <vector>

#include "alog/Logger.h"
#include "autil/Lock.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/service/Session.h"

namespace isearch {
namespace service {

class SessionPool
{
public:
    const static uint32_t defautMaxSessionSize = 400;
public:
    SessionPool(uint32_t maxSessionSize);
    virtual ~SessionPool();
private:
    SessionPool(const SessionPool&);
    SessionPool& operator = (const SessionPool&);
public:
    virtual Session* get() = 0;
    virtual void put(Session* session) = 0;
    uint32_t getMaxSessionSize() const { return _maxSessionSize; }
    uint32_t getCurrentCount() const { return _sessionCount; }
private: //for test
    void setMaxSessionSize(uint32_t maxSessionSize) {
        _maxSessionSize = maxSessionSize;
    }
protected:
    uint32_t _maxSessionSize;
    uint32_t _sessionCount;
private:
    friend class SessionPoolTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SessionPool> SessionPoolPtr;

template<typename SessionType>
class SessionPoolImpl : public SessionPool
{
public:
    SessionPoolImpl(uint32_t maxSessionSize = SessionPool::defautMaxSessionSize)
        : SessionPool(maxSessionSize)
    {
        _sessions = (SessionType*)malloc(sizeof(SessionType) * _maxSessionSize);
        _freeSessions.reserve(_maxSessionSize);
        for (uint32_t i = 0; i < _maxSessionSize; ++i) {
            new (_sessions+i) SessionType(this);
            _freeSessions.push_back(_sessions+i);
        }
    }
    ~SessionPoolImpl() {
        for (uint32_t i = 0; i < _maxSessionSize; ++i) {
            (_sessions+i)->~SessionType();
        }
        free(_sessions);
    }
public:
    SessionType* get() {
        //the max num of session may be _maxSessionSize + num of active threads
        autil::ScopedLock lock(_mutex);
        if (_sessionCount >= _maxSessionSize) {
            AUTIL_LOG(WARN, "Allocate Session failed,the num of sessions has reached limit!");
            return NULL;
        }
        _sessionCount++;
        assert(!_freeSessions.empty());
        SessionType* session = _freeSessions.back();
        _freeSessions.pop_back();
        return session;
    }
    void put(Session* session) {
        assert(NULL != session);
        session->reset();
        autil::ScopedLock lock(_mutex);
        _freeSessions.push_back(static_cast<SessionType*>(session));
        _sessionCount--;
    }
private:
    autil::ThreadMutex _mutex;
    SessionType* _sessions;
    std::vector<SessionType*> _freeSessions;
private:
    friend class SessionPoolTest;
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(ha3, SessionPoolImpl, SessionType);

} // namespace service
} // namespace isearch
