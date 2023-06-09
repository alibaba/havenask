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
#include "ha3/service/Session.h"

#include <iosfwd>

#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/service/SessionPool.h"
#include "suez/turing/common/PoolCache.h"

using namespace std;
namespace isearch {
namespace service {
AUTIL_LOG_SETUP(ha3, Session);

void Session::destroy() {
    if (_sessionPool) {
        _sessionPool->put(this);
    } else {
        delete this;
    }
}

int64_t Session::getCurrentTime() {
    return autil::TimeUtility::currentTime();
}

bool Session::beginSession() {
    if (_poolCache) {
        auto poolCache = _poolCache;
        auto queryPool = _poolCache->get();
        autil::mem_pool::PoolPtr pool(queryPool, [poolCache](autil::mem_pool::Pool *pool){poolCache->put(pool);});
        _pool = pool;
    }
    _sessionMetricsCollectorPtr.reset(new monitor::SessionMetricsCollector());
    _sessionMetricsCollectorPtr->setSessionStartTime(_startTime);
    _sessionMetricsCollectorPtr->beginSessionTrigger();
    return true;
}

void Session::endSession() {
    if (!_sessionMetricsCollectorPtr) {
        return;
    }
    if (_pool) {
        _sessionMetricsCollectorPtr->setMemPoolUsedBufferSize(
                _pool->getAllocatedSize());
        _sessionMetricsCollectorPtr->setMemPoolAllocatedBufferSize(
                _pool->getTotalBytes());
    }
    _sessionMetricsCollectorPtr->endSessionTrigger();
}

void Session::reset() {
    _startTime = 0;
    _sessionMetricsCollectorPtr.reset();
    _gigQuerySession.reset();
    _gigQueryInfo.reset();
    _pool.reset();
    _poolCache = nullptr;
}

} // namespace service
} // namespace isearch
