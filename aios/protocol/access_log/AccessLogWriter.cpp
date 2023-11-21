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
#include "access_log/AccessLogWriter.h"

#include <assert.h>

#include "autil/Log.h"

using namespace std;

AUTIL_DECLARE_AND_SETUP_LOGGER(access_log, AccessLogWriter);

namespace access_log {

AccessLogWriter::AccessLogWriter(const string &logName)
    : AccessLogWriter(logName, {0, autil::RangeUtil::MAX_PARTITION_RANGE}) {}

AccessLogWriter::AccessLogWriter(const string &logName, const autil::PartitionRange &partRange)
    : _logName(logName), _partRange(partRange) {}

AccessLogWriter::~AccessLogWriter() {}

string AccessLogWriter::addHeader(const string &accessLogStr) {
    LenType length = accessLogStr.size();
    assert(sizeof(length) == LOG_HEADER_SIZE);
    return string((char *)&length, sizeof(length)) + accessLogStr;
}

} // namespace access_log
