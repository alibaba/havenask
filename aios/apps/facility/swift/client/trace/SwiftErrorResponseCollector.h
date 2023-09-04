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
#include "autil/Singleton.h"
#include "swift/common/Common.h"

namespace swift {
namespace protocol {
class ReaderInfo;
} // namespace protocol

namespace client {
class SwiftWriter;

enum ErrorResponseType {
    MESSAGE_RESPONSE = 0,
    MESSAGE_RESPONSE_FBMSG_PART,
};

struct ErrorResponseLogHeader {
    uint32_t logLen = 0;
    uint64_t timestamp = 0;
};

class SwiftErrorResponseCollector {
public:
    SwiftErrorResponseCollector(const std::string &logFileName = "logs/swift/err_response.log");

    ~SwiftErrorResponseCollector();

private:
    SwiftErrorResponseCollector(const SwiftErrorResponseCollector &);
    SwiftErrorResponseCollector &operator=(const SwiftErrorResponseCollector &);

public:
    bool logResponse(protocol::ReaderInfo &readerInfo, ErrorResponseType type, const std::string &content);
    void setSwiftWriter(SwiftWriter *writer);
    void resetSwiftWriter();

private:
    void logInFile(const std::string &content);
    void initFileLogger(const std::string &logFileName);

private:
    SwiftWriter *_swiftWriter = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(SwiftErrorResponseCollector);

typedef autil::Singleton<SwiftErrorResponseCollector> ResponseCollectorSingleton;

} // end namespace client
} // end namespace swift
