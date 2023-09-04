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
#ifndef FSLIB_HTTP_PLUGIN_COMMON_H
#define FSLIB_HTTP_PLUGIN_COMMON_H

#include <autil/Lock.h>
#include <autil/Log.h>

#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileLock.h"

#define FSLIB_PLUGIN_BEGIN_NAMESPACE(x)                                                                                \
    namespace fslib {                                                                                                  \
    namespace fs {                                                                                                     \
    namespace x {
#define FSLIB_PLUGIN_END_NAMESPACE(x)                                                                                  \
    }                                                                                                                  \
    }                                                                                                                  \
    }
#define FSLIB_PLUGIN_USING_NAMESPACE(x) using namespace fslib::fs::x

#define CALL_WITH_CHECK(funcName, ...)                                                                                 \
    do {                                                                                                               \
        CURLcode res;                                                                                                  \
        res = funcName(__VA_ARGS__);                                                                                   \
        if (res != CURLE_OK) {                                                                                         \
            AUTIL_LOG(ERROR, "error when call " #funcName ": [%s]", curl_easy_strerror(res));                          \
        }                                                                                                              \
    } while (0)

#endif // FSLIB_HTTP_PLUGIN_COMMON_H
