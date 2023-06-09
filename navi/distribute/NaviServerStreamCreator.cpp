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
#include "navi/distribute/NaviServerStreamCreator.h"
#include "navi/distribute/NaviServerStream.h"
#include "navi/engine/Navi.h"
#include "navi/engine/NaviSnapshot.h"

using namespace autil;

namespace navi {

NaviServerStreamCreator::NaviServerStreamCreator(const NaviLoggerPtr &logger, Navi *navi)
    : _navi(navi)
{
    atomic_set(&_queryCount, 0);
    _logger.logger = logger;
    _logger.object = this;
    _logger.prefix.clear();
}

NaviServerStreamCreator::~NaviServerStreamCreator() {
}

std::string NaviServerStreamCreator::methodName() const {
    return NAVI_RPC_METHOD_NAME;
}

multi_call::GigServerStreamPtr NaviServerStreamCreator::create() {
    if (incQueryCount()) {
        return multi_call::GigServerStreamPtr(new NaviServerStream(this, _navi));
    } else {
        return nullptr;
    }
}

bool NaviServerStreamCreator::incQueryCount() {
    ScopedReadLock lock(_rwLock);
    if (!_navi) {
        NAVI_LOG(SCHEDULE2, "creator is stopped, queryCount [%lld]", atomic_read(&_queryCount));
        return false;
    }
    NAVI_LOG(SCHEDULE2, "inc queryCount from [%lld]", atomic_read(&_queryCount));
    atomic_inc(&_queryCount);
    return true;
}

void NaviServerStreamCreator::decQueryCount() {
    NAVI_LOG(SCHEDULE2, "dec queryCount from [%lld]", atomic_read(&_queryCount));
    atomic_dec(&_queryCount);
}

void NaviServerStreamCreator::stop() {
    NAVI_LOG(INFO, "start stop navi server stream");
    {
        ScopedWriteLock lock(_rwLock);
        _navi = nullptr;
    }
    size_t sleepCount = 0;
    while (true) {
        auto queryCount = atomic_read(&_queryCount);
        if (queryCount <= 0) {
            NAVI_LOG(INFO, "wait queryCount succeed, sleepCount [%lu]", sleepCount);
            break;
        }
        if (sleepCount++ % 200 == 0) {
            NAVI_LOG(INFO, "still has queryCount [%lld], sleep", queryCount);
        }
        usleep(10 * 1000);
    }
    NAVI_LOG(INFO, "finish stop navi server stream");
}

}

