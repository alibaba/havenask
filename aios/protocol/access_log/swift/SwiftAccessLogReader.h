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
#ifndef ISEARCH_ACCESS_LOG_SWIFTACCESSLOGREADER_H
#define ISEARCH_ACCESS_LOG_SWIFTACCESSLOGREADER_H

#include <limits>

#include "access_log/AccessLogReader.h"

namespace swift {
namespace client {
class SwiftReader;
} // namespace client
} // namespace swift

namespace access_log {

class SwiftAccessLogReader : public AccessLogReader {
public:
    SwiftAccessLogReader(const std::string &logName, swift::client::SwiftReader *swiftReader);
    ~SwiftAccessLogReader();

private:
    SwiftAccessLogReader(const SwiftAccessLogReader &);
    SwiftAccessLogReader &operator=(const SwiftAccessLogReader &);

public:
    bool read(uint32_t count,
              std::vector<PbAccessLog> &accessLogs,
              int32_t statusCode = std::numeric_limits<int32_t>::min()) override;

    bool readCustomLog(uint32_t count, std::vector<std::string> &customLogs) override;

    std::string getType() const override { return SWIFT_TYPE; }

private:
    bool doRead(uint32_t count, std::vector<PbAccessLog> &accessLogs, int32_t statusCode);

    bool doReadCustomLog(uint32_t count, std::vector<std::string> &customLogs);

    int32_t getStartMessageId(uint32_t count);

    bool seek(uint32_t count);

private:
    swift::client::SwiftReader *_swiftReader;
};
} // namespace access_log

#endif // ISEARCH_ACCESS_LOG_SWIFTACCESSLOGREADER_H
