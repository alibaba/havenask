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
#include "access_log/swift/SwiftAccessLogReader.h"

#include <limits>
#include <unistd.h>

#include "autil/Log.h"
#include "autil/legacy/json.h"
#include "swift/client/MessageInfo.h"
#include "swift/client/SwiftReader.h"

using namespace std;
using namespace swift::client;
using namespace swift::protocol;

AUTIL_DECLARE_AND_SETUP_LOGGER(access_log, SwiftAccessLogReader);

namespace access_log {

SwiftAccessLogReader::SwiftAccessLogReader(const string &logName, SwiftReader *swiftReader)
    : AccessLogReader(logName), _swiftReader(swiftReader) {}

SwiftAccessLogReader::~SwiftAccessLogReader() {
    if (_swiftReader) {
        delete _swiftReader;
    }
}

bool SwiftAccessLogReader::read(uint32_t count, vector<PbAccessLog> &accessLogs, int32_t statusCode) {
    accessLogs.reserve(count);

    if (!seek(count)) {
        return false;
    }

    if (!doRead(count, accessLogs, statusCode)) {
        return false;
    }

    if (accessLogs.size() == 0) {
        AUTIL_LOG(ERROR, "Read log for %s failed", _logName.c_str());
        return false;
    }

    return true;
}

bool SwiftAccessLogReader::readCustomLog(uint32_t count, vector<string> &customLogs) {
    if (!seek(count)) {
        return false;
    }

    if (!doReadCustomLog(count, customLogs)) {
        return false;
    }

    if (customLogs.size() == 0) {
        AUTIL_LOG(ERROR, "Read log for %s failed", _logName.c_str());
        return false;
    }
    return true;
}

bool SwiftAccessLogReader::doRead(uint32_t count, vector<PbAccessLog> &accessLogs, int32_t statusCode) {
    uint32_t retryTimes = 0;
    for (size_t i = 0; i < count; i++) {
        Message msg;
        ErrorCode errorCode = _swiftReader->read(msg);
        // TODO: how to handle error
        if (errorCode == ERROR_CLIENT_NO_MORE_MESSAGE) {
            return true;
        } else if (errorCode != ERROR_NONE) {
            usleep(100 * 1000);
            if (++retryTimes >= AccessLogReader::MAX_RETRY_TIME) {
                AUTIL_LOG(ERROR, "retry read log from swift exceed MAX_RETRY_TIME.");
                return false;
            }
        } else {
            PbAccessLog pbLog;
            if (!pbLog.ParseFromString(msg.data())) {
                AUTIL_LOG(WARN, "parse pb log failed");
                continue;
            }
            if (statusCode == std::numeric_limits<int32_t>::min() || pbLog.statuscode() == statusCode) {
                accessLogs.push_back(pbLog);
            }
        }
    }
    return true;
}

bool SwiftAccessLogReader::doReadCustomLog(uint32_t count, std::vector<std::string> &customLogs) {
    uint32_t retryTimes = 0;
    for (size_t i = 0; i < count; i++) {
        Message msg;
        ErrorCode errorCode = _swiftReader->read(msg);
        // TODO: how to handle error
        if (errorCode == ERROR_CLIENT_NO_MORE_MESSAGE) {
            return true;
        } else if (errorCode != ERROR_NONE) {
            usleep(100 * 1000);
            if (++retryTimes >= AccessLogReader::MAX_RETRY_TIME) {
                AUTIL_LOG(ERROR, "retry read log from swift exceed MAX_RETRY_TIME.");
                return false;
            }
        } else {
            customLogs.push_back(msg.data());
        }
    }
    return true;
}

bool SwiftAccessLogReader::seek(uint32_t count) {
    // seek
    int32_t startMessageId = getStartMessageId(count);
    if (startMessageId == -1) {
        return false;
    }

    ErrorCode ec;
    ec = _swiftReader->seekByMessageId(startMessageId);
    if (ec != ERROR_NONE) {
        AUTIL_LOG(ERROR, "swift reader seekByMessageId failed, error code[%d]", int32_t(ec));
        return false;
    }
    return true;
}

int32_t SwiftAccessLogReader::getStartMessageId(uint32_t count) {
    SwiftPartitionStatus status;
    uint32_t retry = 0;
    int64_t maxMessageId = -1;

    while (retry < 5) {
        status = _swiftReader->getSwiftPartitionStatus();
        if (status.refreshTime != -1) {
            maxMessageId = status.maxMessageId;
            break;
        }
        ++retry;
        usleep(500 * 1000);
    }
    if (maxMessageId == -1) {
        AUTIL_LOG(ERROR, "get swift partition status failed");
        return -1;
    }

    return maxMessageId >= count ? maxMessageId - count + 1 : 0;
}
} // namespace access_log
