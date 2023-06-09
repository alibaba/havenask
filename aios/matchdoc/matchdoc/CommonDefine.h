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
#ifndef ISEARCH_COMMON_DEFINE_H
#define ISEARCH_COMMON_DEFINE_H
#include <limits>
#include <stdint.h>

static const uint64_t INVALID_OFFSET = std::numeric_limits<uint64_t>::max();
static const uint32_t INVALID_MOUNT_ID = std::numeric_limits<uint32_t>::max();

enum SerializeLevel {
    SL_NONE = 0,              // not serialize
    SL_RTP_PROXY = 2,         // for RTP
    SL_RTP_FRONTEND = 4,      // for RTP
    SL_PROXY = 10,            // serialize value for proxy/qrs
    SL_CACHE = 20,            // serialize value for cache/proxy/qrs
    SL_FRONTEND = 30,         // serialize value for sp/proxy/qrs
    SL_ATTRIBUTE = 40,        // serialize value for sp/cache/proxy/qrs
    SL_VARIABLE = 50,         // serialize value for frontend/sp/cache/proxy/qrs
    SL_MAX = 255
};

#endif //ISEARCH_COMMON_DEFINE_H
