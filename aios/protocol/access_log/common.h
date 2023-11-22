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
#ifndef ISEARCH_ACCESS_LOG_COMMON_H
#define ISEARCH_ACCESS_LOG_COMMON_H

#include <string>
#include <vector>
#include <unordered_map>

namespace access_log {
static const std::string LOCAL_TYPE = "local";
static const std::string SWIFT_TYPE = "swift";

typedef uint32_t LenType;

static const uint32_t LOG_HEADER_SIZE = 4;
}

#endif //ISEARCH_ACCESS_LOG_COMMON_H
