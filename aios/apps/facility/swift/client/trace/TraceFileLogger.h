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

#include <string>

#include "autil/Log.h"
#include "swift/client/trace/TraceLogger.h"
#include "swift/common/Common.h"

namespace swift {
namespace client {

class TraceFileLogger : public TraceLogger {
public:
    TraceFileLogger();
    ~TraceFileLogger();

private:
    TraceFileLogger(const TraceFileLogger &);
    TraceFileLogger &operator=(const TraceFileLogger &);

public:
    static bool initLogger(const std::string &logFile = "logs/swift/trace.log");

public:
    bool flush() override;

protected:
    bool doWrite(const std::string &content) override;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(TraceFileLogger);

} // end namespace client
} // end namespace swift
