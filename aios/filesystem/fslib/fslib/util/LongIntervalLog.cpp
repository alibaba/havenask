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
#include "autil/TimeUtility.h"
#ifdef ENABLE_BEEPER
#include "beeper/beeper.h"
#endif
#include "fslib/util/LongIntervalLog.h"

using namespace std;
using namespace autil;
FSLIB_BEGIN_NAMESPACE(util);

LongIntervalLog::LongIntervalLog(
    const char *file, int line, const char *func, const char *msg, int64_t interval, alog::Logger *logger)
    : _file(file), _line(line), _func(func), _msg(msg), _interval(interval), _logger(logger) {
    _beginTime = TimeUtility::currentTime();
}

LongIntervalLog::~LongIntervalLog() {
    int64_t endTime = TimeUtility::currentTime();
    int64_t t = endTime - _beginTime;
    if (t >= _interval) {
        _logger->log(alog::LOG_LEVEL_INFO, _file, _line, _func, "interval [%ld], %s", t, _msg);
#ifdef ENABLE_BEEPER
        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(
            FSLIB_LONG_INTERVAL_COLLECTOR_NAME, "%s:%d:%s, interval [%ld], %s", _file, _line, _func, t, _msg);
#endif
    }
#ifdef ENABLE_BEEPER
    if (t >= 10 * 1000 * 1000) { // 10s
        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(
            FSLIB_ERROR_COLLECTOR_NAME, "%s:%d:%s, interval [%ld], %s", _file, _line, _func, t, _msg);
    }
#endif
}

FSLIB_END_NAMESPACE(util);
