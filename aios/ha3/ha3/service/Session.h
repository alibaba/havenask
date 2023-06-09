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
#include <stdint.h>
#include <memory>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/mem_pool/Pool.h"
#include "ha3/isearch.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "suez/turing/common/PoolCache.h"

namespace suez {
namespace turing {
class PoolCache;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace service {

class SessionPool;

class Session
{
public:
    Session(SessionPool *pool)
        : _sessionPool(pool)
        , _startTime(0)
        , _sessionSrcType(SESSION_SRC_UNKNOWN)
        , _useGigSrc(false)
    {
    }
    virtual ~Session() {}
private:
    Session(const Session &);
    Session& operator = (const Session &);
public:
    // either processRequest or dropRequest should be called after create
    virtual void processRequest() = 0;
    virtual void dropRequest() = 0;
    virtual bool beginSession();
    virtual void endSession();
    virtual void reset();
public:
    void setStartTime(int64_t t = getCurrentTime()) {
        _startTime = t;
    };
    int64_t getStartTime() const {
        return _startTime;
    };
    void setSessionSrcType(SessionSrcType type) {
        _sessionSrcType = type;
    }
    SessionSrcType getSessionSrcType() {
        return _sessionSrcType;
    }
    void setGigQuerySession(const multi_call::QuerySessionPtr &querySession) {
        _gigQuerySession = querySession;
    }
    multi_call::QuerySessionPtr getGigQuerySession() {
        return _gigQuerySession;
    }
    void setUseGigSrc(bool flag) {
        _useGigSrc = flag;
    }
    void setPoolCache(suez::turing::PoolCache *poolCache) {
        _poolCache = poolCache;
    }
public:
    static int64_t getCurrentTime();
    autil::mem_pool::Pool *getMemPool() { return _pool.get(); }
protected:
    // can only be destroyed by sub-class
    void destroy();
protected:
    // use to return self back
    SessionPool *_sessionPool;
    int64_t _startTime;
    SessionSrcType _sessionSrcType;
    bool _useGigSrc = false;
    suez::turing::PoolCache *_poolCache = nullptr;
    autil::mem_pool::PoolPtr _pool;
    monitor::SessionMetricsCollectorPtr _sessionMetricsCollectorPtr;
    multi_call::QuerySessionPtr _gigQuerySession;
    multi_call::QueryInfoPtr _gigQueryInfo;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<Session> SessionPtr;

} // namespace service
} // namespace isearch
