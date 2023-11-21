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
#include "access_log/AccessLogReader.h"

#include "autil/Log.h"

using namespace std;

AUTIL_DECLARE_AND_SETUP_LOGGER(access_log, AccessLogReader);

namespace access_log {

const uint32_t AccessLogReader::MAX_RETRY_TIME = 20;
const uint32_t AccessLogReader::MAX_LOG_LENGTH = 100 * 1024 * 1024; // 100M

AccessLogReader::AccessLogReader(const string &logName) : _logName(logName) {}

AccessLogReader::~AccessLogReader() {}

int64_t AccessLogReader::parseHeader(char *header) {
    if (NULL == header) {
        return -1;
    }

    uint32_t value = *(reinterpret_cast<uint32_t *>(header));
    if (value > MAX_LOG_LENGTH) {
        AUTIL_LOG(WARN, "log length exceed limit max log length");
        return -1;
    } else if (value == 0) {
        AUTIL_LOG(WARN, "log length equal 0, return -1");
        return -1;
    }

    return int64_t(value);
}

} // namespace access_log
