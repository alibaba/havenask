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
#ifndef ISEARCH_ACCESS_LOG_LOCALACCESSLOGACCESSOR_H
#define ISEARCH_ACCESS_LOG_LOCALACCESSLOGACCESSOR_H

#include "access_log/common.h"
#include "access_log/AccessLogAccessor.h"

namespace access_log {

class LocalAccessLogAccessor : public AccessLogAccessor
{
public:
    LocalAccessLogAccessor(const std::string &logName,
                           const std::string &logDir="");
    ~LocalAccessLogAccessor();
private:
    LocalAccessLogAccessor(const LocalAccessLogAccessor &) = delete;
    LocalAccessLogAccessor& operator=(const LocalAccessLogAccessor &) = delete;
public:
    AccessLogWriter *createWriter();
    AccessLogReader *createReader();

private:
    std::string _logDir;
};

}

#endif //ISEARCH_ACCESS_LOG_LOCALACCESSLOGACCESSOR_H
