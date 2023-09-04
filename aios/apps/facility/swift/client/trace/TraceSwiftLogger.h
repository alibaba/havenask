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

#include "autil/Log.h"
#include "swift/client/trace/TraceLogger.h"
#include "swift/common/Common.h"

namespace swift {
namespace client {
class SwiftWriter;

struct MessageMeta {
    std::string topicName;
    uint16_t hashVal = 0;
    uint8_t mask = 0;
};

class TraceSwiftLogger : public TraceLogger {
public:
    TraceSwiftLogger(const MessageMeta &meta, SwiftWriter *tracingWriter);
    ~TraceSwiftLogger();

private:
    TraceSwiftLogger(const TraceSwiftLogger &);
    TraceSwiftLogger &operator=(const TraceSwiftLogger &);

public:
    bool flush() override;

protected:
    bool doWrite(const std::string &content) override;

private:
    MessageMeta _msgMeta;
    SwiftWriter *_tracingWriter = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(TraceSwiftLogger);

} // end namespace client
} // end namespace swift
