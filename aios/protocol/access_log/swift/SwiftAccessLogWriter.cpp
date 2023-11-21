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
#include "access_log/swift/SwiftAccessLogWriter.h"

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/jsonizable.h"

using namespace std;
using namespace swift::client;
using namespace swift::protocol;
using namespace autil;
using namespace autil::legacy;

AUTIL_DECLARE_AND_SETUP_LOGGER(access_log, SwiftAccessLogWriter);

namespace access_log {

SwiftAccessLogWriter::SwiftAccessLogWriter(const string &logName, const PartitionRange &partRange, SwiftWriter *writer)
    : AccessLogWriter(logName), _partRange(partRange), _swiftWriter(writer) {}

SwiftAccessLogWriter::~SwiftAccessLogWriter() {
    if (_swiftWriter) {
        delete _swiftWriter;
    }
}

bool SwiftAccessLogWriter::write(const string &accessLogStr) {
    MessageInfo msg;
    msg.data = accessLogStr;
    // 如果swift分了多列, 使用range.first只能用上第一列
    // 如果写入qps较大, 可以改成random到(range.first, range.second)
    msg.uint16Payload = _partRange.first;
    if (!_swiftWriter) {
        AUTIL_LOG(ERROR, "swiftWriter is nullptr");
        return false;
    }
    auto ec = _swiftWriter->write(msg);
    if (ec != ERROR_NONE) {
        AUTIL_LOG(ERROR, "write swift log failed, ec[%d]", int(ec));
        return false;
    }
    return true;
}
} // namespace access_log
