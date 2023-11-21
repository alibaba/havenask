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
#ifndef ISEARCH_ACCESS_LOG_SWIFTACCESSLOGWRITER_H
#define ISEARCH_ACCESS_LOG_SWIFTACCESSLOGWRITER_H
#include "access_log/AccessLogWriter.h"
#include "autil/StringUtil.h"
#include "swift/client/MessageInfo.h"
#include "swift/client/SwiftClient.h"

namespace access_log {

class SwiftAccessLogWriter : public AccessLogWriter {
public:
    SwiftAccessLogWriter(const std::string &logName,
                         const autil::PartitionRange &range,
                         swift::client::SwiftWriter *writer);
    ~SwiftAccessLogWriter();

private:
    SwiftAccessLogWriter(const SwiftAccessLogWriter &);
    SwiftAccessLogWriter &operator=(const SwiftAccessLogWriter &);

public:
    bool write(const std::string &accessLogStr) override;

    std::string getType() const override { return SWIFT_TYPE; }
    bool active() { return true; }

private:
    const autil::PartitionRange _partRange;
    swift::client::SwiftWriter *_swiftWriter;
};
} // namespace access_log

#endif // ISEARCH_ACCESS_LOG_SWIFTACCESSLOGWRITER_H
