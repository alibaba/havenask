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

#include <stdint.h>
#include <string>

#include "autil/TimeUtility.h"
#include "swift/common/Common.h"

namespace swift {
namespace client {

struct LogHeader {
    LogHeader() : timestamp(0), logLen(0), reserved(0) {}
    int64_t timestamp;
    int64_t logLen   : 40;
    int64_t reserved : 24;
};

class TraceLogger {
public:
    TraceLogger(){};
    virtual ~TraceLogger(){};

private:
    TraceLogger(const TraceLogger &);
    TraceLogger &operator=(const TraceLogger &);

public:
    bool write(const std::string &content) {
        std::string str = addHeader(content);
        return doWrite(str);
    }
    virtual bool flush() = 0;

protected:
    virtual bool doWrite(const std::string &content) = 0;

protected:
    std::string addHeader(const std::string &content) {
        LogHeader header;
        header.logLen = content.size();
        header.timestamp = autil::TimeUtility::currentTime();
        return std::string((char *)&header, sizeof(header)) + content;
    }
};

SWIFT_TYPEDEF_PTR(TraceLogger);
} // namespace client
} // namespace swift
