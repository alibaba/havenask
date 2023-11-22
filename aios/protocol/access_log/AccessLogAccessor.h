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
#ifndef ISEARCH_ACCESS_LOG_ACCESSLOGACCESSOR_H
#define ISEARCH_ACCESS_LOG_ACCESSLOGACCESSOR_H

#include "access_log/AccessLogReader.h"
#include "access_log/AccessLogWriter.h"
#include "access_log/common.h"

namespace access_log {
class AccessLogAccessor {
public:
    AccessLogAccessor(const std::string &logName);
    virtual ~AccessLogAccessor();

private:
    AccessLogAccessor(const AccessLogAccessor &);
    AccessLogAccessor &operator=(const AccessLogAccessor &);

public:
    virtual AccessLogWriter *createWriter() = 0;
    virtual AccessLogReader *createReader() = 0;
    virtual bool init() { return true; }

protected:
    const std::string _logName;
};

} // namespace access_log

#endif // ISEARCH_ACCESS_LOG_ACCESSLOGACCESSOR_H
