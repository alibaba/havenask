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
#ifndef ISEARCH_ACCESS_LOG_ACCESSLOGWRITER_H
#define ISEARCH_ACCESS_LOG_ACCESSLOGWRITER_H

#include "access_log/common.h"
#include "autil/RangeUtil.h"

namespace access_log {

class AccessLogWriter {
public:
    AccessLogWriter(const std::string &logName, const autil::PartitionRange &partRange);
    AccessLogWriter(const std::string &logName);
    virtual ~AccessLogWriter();

private:
    AccessLogWriter(const AccessLogWriter &) = delete;
    AccessLogWriter &operator=(const AccessLogWriter &) = delete;

public:
    virtual bool init() { return true; }

    virtual bool write(const std::string &pbAccessLogStr) = 0;

    virtual std::string getType() const = 0;

public:
    static std::string addHeader(const std::string &accessLogStr);

protected:
    bool isActive() const { return true; }

    virtual bool recover() { return true; }

protected:
    const std::string _logName;
    const autil::PartitionRange _partRange;
};
} // namespace access_log

#endif // ISEARCH_ACCESS_LOG_ACCESSLOGWRITER_H
