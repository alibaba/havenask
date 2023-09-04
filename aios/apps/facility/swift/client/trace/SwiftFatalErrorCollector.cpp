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
#include "swift/client/trace/SwiftFatalErrorCollector.h"

#include <iosfwd>

#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;
namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, SwiftFatalErrorCollector);

SwiftFatalErrorCollector::SwiftFatalErrorCollector() {}

SwiftFatalErrorCollector::~SwiftFatalErrorCollector() {}

void SwiftFatalErrorCollector::addRequestHasErrorDataInfo(const std::string &errorInfo) {
    AUTIL_LOG(ERROR, "add fatal error: %s", errorInfo.c_str());
    ScopedWriteLock wlock(_rwlock);
    _requestFatalCount++;
    if (_requestFatalQueue.size() > 1000) {
        _requestFatalQueue.pop_front();
    }
    _requestFatalQueue.push_back(errorInfo);
}

int64_t SwiftFatalErrorCollector::getTotalRequestFatalCount() {
    ScopedReadLock rlock(_rwlock);
    return _requestFatalCount;
}

deque<std::string> SwiftFatalErrorCollector::getLastestRequestFatalInfo() {
    ScopedReadLock rlock(_rwlock);
    return _requestFatalQueue;
}

} // end namespace client
} // end namespace swift
