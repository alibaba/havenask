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
#ifndef ISEARCH_ACCESS_LOG_LOCALACCESSLOGWRITER_H
#define ISEARCH_ACCESS_LOG_LOCALACCESSLOGWRITER_H

#include "access_log/AccessLogWriter.h"

namespace access_log {

class LocalAccessLogWriter : public AccessLogWriter {
public:
    LocalAccessLogWriter(const std::string &logName);
    ~LocalAccessLogWriter();

private:
    LocalAccessLogWriter(const LocalAccessLogWriter &);
    LocalAccessLogWriter &operator=(const LocalAccessLogWriter &);

public:
    bool init() override { return true; }

    bool write(const std::string &accessLogStr) override;

    std::string getType() const override { return SWIFT_TYPE; }
};
} // namespace access_log

#endif // ISEARCH_ACCESS_LOG_LOCALACCESSLOGWRITER_H
