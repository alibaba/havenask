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
#ifndef FSLIB_LONGINTERVALLOG_H
#define FSLIB_LONGINTERVALLOG_H

#include "alog/Logger.h"
#include <stdint.h>
#include <map>
#include <string>
#include "fslib/common/common_type.h"
#include "fslib/common/common_define.h"

FSLIB_BEGIN_NAMESPACE(util);

class LongIntervalLog
{
public:
    LongIntervalLog(const char *file, int line, const char *func,
                    const char* msg, int64_t interval,
                    alog::Logger *logger);
    ~LongIntervalLog();
private:
    const char *_file;
    int _line;
    const char *_func;
    const char *_msg;
    int64_t _interval;
    alog::Logger *_logger;
    int64_t _beginTime;
};

FSLIB_END_NAMESPACE(util);

#define FSLIB_LONG_INTERVAL_LOG_INTERVAL(interval, format, args...)     \
    char __fslib_long_interval_log_buf[4096];                           \
    (void) snprintf(__fslib_long_interval_log_buf,                      \
                    sizeof(__fslib_long_interval_log_buf),              \
                    format, ##args);                                    \
    fslib::util::LongIntervalLog __longItervalLog(                      \
            __FILE__, __LINE__, __FUNCTION__,                           \
            __fslib_long_interval_log_buf, interval, _logger);

#define FSLIB_LONG_INTERVAL_LOG(format, args...)                        \
    FSLIB_LONG_INTERVAL_LOG_INTERVAL((1000 * 1000), format, ##args)

#endif //FSLIB_LONGINTERVALLOG_H
