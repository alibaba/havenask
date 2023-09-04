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
#include "sql/ops/util/AsyncCallbackCtxBase.h"

#include <assert.h>
#include <cstddef>

#include "autil/TimeUtility.h"
#include "navi/common.h"
#include "navi/log/NaviLogger.h"
#include "sql/common/Log.h" // IWYU pragma: keep

using namespace std;
using namespace autil;
using namespace navi;

namespace sql {

AsyncCallbackCtxBase::AsyncCallbackCtxBase()
    : _logger(NAVI_TLS_LOGGER ? *NAVI_TLS_LOGGER : NaviObjectLogger {}) {}

AsyncCallbackCtxBase::~AsyncCallbackCtxBase() {}

void AsyncCallbackCtxBase::incStartVersion() {
    size_t version;
    {
        ScopedSpinLock scope(_statLock);
        assert(_startVersion == _callbackVersion);
        version = ++_startVersion;
    }
    _startTime = TimeUtility::monotonicTimeUs();
    NAVI_LOG(TRACE3, "startTime[%ld] version[%lu]", _startTime, version);
}

int64_t AsyncCallbackCtxBase::incCallbackVersion() {
    size_t version;
    {
        ScopedSpinLock scope(_statLock);
        version = ++_callbackVersion;
        assert(_startVersion == _callbackVersion);
    }
    size_t durTime = TimeUtility::monotonicTimeUs() - _startTime;
    NAVI_LOG(TRACE3, "durTime[%ld] version[%lu]", durTime, version);
    return durTime;
}

bool AsyncCallbackCtxBase::isInFlightNoLock() const {
    assert(_startVersion == _callbackVersion || _startVersion == _callbackVersion + 1);
    return _startVersion == _callbackVersion + 1;
}

} // namespace sql
