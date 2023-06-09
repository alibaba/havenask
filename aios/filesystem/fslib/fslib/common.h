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

#include "autil/Log.h"
#include "autil/Lock.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileLock.h"
#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"

#define FSLIB_PLUGIN_BEGIN_NAMESPACE(x) namespace fslib { namespace fs \
    { namespace x {
#define FSLIB_PLUGIN_END_NAMESPACE(x) } } }
#define FSLIB_PLUGIN_USING_NAMESPACE(x) using namespace fslib::fs::x

#define FSLIB_PANGU_PLUGIN_BEGIN_NAMESPACE(x)   \
    namespace fslib {                           \
    namespace fs {                              \
    namespace pangu {                           \
    namespace x {

#define FSLIB_PANGU_PLUGIN_END_NAMESPACE(x)     \
    }                                           \
        }                                       \
        }                                       \
        }
#define FSLIB_PANGU_PLUGIN_USING_NAMESPACE(x) using namespace fslib::fs::pangu::x
#define FSLIB_PANGU_PLUGIN_NAMESPACE(x) fslib::fs::pangu::x

#if 0
#define FSLIB_PANGU_LONG_INTERVAL_LOG(format, args...)                  \
    int64_t intervalLog_beginTime(TimeUtility::currentTime());          \
    auto intervalLog_deleter = [&](int64_t* b) {                        \
        int64_t i = TimeUtility::currentTime() - *b;                    \
        if (i >= 1000 * 1000) {                                         \
            AUTIL_LOG(INFO, "interval [%ld], " format, i, ##args);      \
        }                                                               \
    };                                                                  \
    unique_ptr<int64_t, decltype(intervalLog_deleter)> intervalLog_guard(&intervalLog_beginTime, intervalLog_deleter);
#else
#define FSLIB_PANGU_LONG_INTERVAL_LOG(format, args...) FSLIB_LONG_INTERVAL_LOG_INTERVAL((1000 * 1000), format, ##args)
#endif

#define CALL_WITH_CHECK(funcName, ...)                          \
    do {                                                        \
    CURLcode res;                                               \
    res = funcName(__VA_ARGS__);                                \
    if (res != CURLE_OK) {                                      \
        AUTIL_LOG(ERROR, "error when call " #funcName ": [%s]", \
                  curl_easy_strerror(res));                     \
    }                                                           \
    } while (0)

#define HAVE_ALOG
#define HADOOP_CDH
