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
#ifndef ISEARCH_ACCESS_LOG_ACCESSLOGREADER_H
#define ISEARCH_ACCESS_LOG_ACCESSLOGREADER_H

#include "access_log/PbAccessLog.pb.h"
#include "access_log/common.h"
#include "autil/RangeUtil.h"

namespace access_log {

class AccessLogReader {
public:
    AccessLogReader(const std::string &logName);
    virtual ~AccessLogReader();

private:
    AccessLogReader(const AccessLogReader &);
    AccessLogReader &operator=(const AccessLogReader &);

public:
    virtual bool init() { return true; }

    // status code == INT_MIN means get all logs, or just get the special logs
    virtual bool read(uint32_t count, std::vector<PbAccessLog> &accessLogs, int32_t statusCode = INT_MIN) = 0;

    // read custom log
    virtual bool readCustomLog(uint32_t count, std::vector<std::string> &accessLogs) = 0;

    virtual std::string getType() const = 0;

    static int64_t parseHeader(char *header);

    bool isActive() const { return true; }

    virtual bool recover() { return true; }

    // need release resource after read if it takes long time util next read
    virtual void releaseResource() {}

protected:
    const std::string _logName;
    static const uint32_t MAX_RETRY_TIME;
    static const uint32_t MAX_LOG_LENGTH;
};
} // namespace access_log

#endif // ISEARCH_ACCESS_LOG_ACCESSLOGREADER_H
