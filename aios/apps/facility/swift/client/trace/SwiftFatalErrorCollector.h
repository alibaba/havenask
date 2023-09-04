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

#include <deque>
#include <stdint.h>
#include <string>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/Singleton.h"
#include "swift/common/Common.h"

namespace swift {
namespace client {

class SwiftFatalErrorCollector {
public:
    SwiftFatalErrorCollector();
    ~SwiftFatalErrorCollector();

private:
    SwiftFatalErrorCollector(const SwiftFatalErrorCollector &);
    SwiftFatalErrorCollector &operator=(const SwiftFatalErrorCollector &);

public:
    void addRequestHasErrorDataInfo(const std::string &errorInfo);
    int64_t getTotalRequestFatalCount();
    std::deque<std::string> getLastestRequestFatalInfo();

private:
private:
    int64_t _requestFatalCount = 0;
    std::deque<std::string> _requestFatalQueue;
    autil::ReadWriteLock _rwlock;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(SwiftFatalErrorCollector);

typedef autil::Singleton<SwiftFatalErrorCollector> ErrorCollectorSingleton;

} // end namespace client
} // end namespace swift
