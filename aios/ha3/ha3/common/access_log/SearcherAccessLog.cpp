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
#include "ha3/common/access_log/SearcherAccessLog.h" // IWYU pragma: keep

#include <iosfwd>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;
namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, SearcherAccessLog);

SearcherAccessLog::SearcherAccessLog()
    : _entryTime(TimeUtility::currentTime())
{
}

SearcherAccessLog::~SearcherAccessLog() {
    _exitTime = TimeUtility::currentTime();
    log();
}

void SearcherAccessLog::log() {
    AUTIL_LOG(INFO,
              "%ld->%ld %ldus %s {{{ %s }}}",
              _entryTime,
              _exitTime,
              _exitTime - _entryTime,
              _eTraceId.c_str(),
              _payload.c_str());
}

} // namespace common
} // namespace isearch
