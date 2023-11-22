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
#ifndef ISEARCH_ACCESS_LOG_ACCESSLOGACCESSORFACTORY_H
#define ISEARCH_ACCESS_LOG_ACCESSLOGACCESSORFACTORY_H

#include "access_log/AccessLogAccessor.h"
#include "access_log/common.h"
#include "autil/legacy/any.h"
#include "autil/legacy/json.h"

namespace access_log {

class AccessLogAccessorFactory {
public:
    AccessLogAccessorFactory();
    ~AccessLogAccessorFactory();

private:
    AccessLogAccessorFactory(const AccessLogAccessorFactory &) = delete;
    AccessLogAccessorFactory &operator=(const AccessLogAccessorFactory &) = delete;

public:
    static AccessLogAccessor *getAccessor(const std::string &logName,
                                          const std::string &typeName,
                                          const autil::legacy::Any &config,
                                          const autil::PartitionRange &partRange);
};

} // namespace access_log

#endif // ISEARCH_ACCESS_LOG_ACCESSLOGACCESSORFACTORY_H
