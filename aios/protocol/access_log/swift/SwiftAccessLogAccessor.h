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
#ifndef ISEARCH_ACCESS_LOG_SWIFTACCESSLOGACCESSOR_H
#define ISEARCH_ACCESS_LOG_SWIFTACCESSLOGACCESSOR_H

#include "access_log/AccessLogAccessor.h"
#include "access_log/common.h"
#include "access_log/swift/SwiftAccessLogConfig.h"
#include "access_log/swift/SwiftAccessLogReader.h"
#include "access_log/swift/SwiftAccessLogWriter.h"

namespace swift {
namespace client {
class SwiftClient;
} // namespace client
} // namespace swift

namespace access_log {

class SwiftAccessLogAccessor : public AccessLogAccessor {
public:
    SwiftAccessLogAccessor(const std::string &logName,
                           const autil::legacy::Any &config,
                           const autil::PartitionRange &partRange);
    ~SwiftAccessLogAccessor();

private:
    SwiftAccessLogAccessor(const SwiftAccessLogAccessor &) = delete;
    SwiftAccessLogAccessor &operator=(const SwiftAccessLogAccessor &) = delete;

public:
    bool init() override;

    AccessLogWriter *createWriter() override;

    AccessLogReader *createReader() override;

private:
    swift::client::SwiftWriter *createSwiftWriter();
    swift::client::SwiftReader *createSwiftReader();

private:
    std::string _zkPath;
    autil::legacy::Any _config;
    autil::PartitionRange _partRange;
    SwiftAccessLogConfig _swiftConfig;
    swift::client::SwiftClient *_swiftClient;
    static const std::string DEFAULT_SITE;
};

} // namespace access_log

#endif // ISEARCH_ACCESS_LOG_SWIFTACCESSLOGACCESSOR_H
