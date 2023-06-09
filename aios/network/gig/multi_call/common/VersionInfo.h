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
#ifndef ISEARCH_MULTI_CALL_VERSIONINFO_H
#define ISEARCH_MULTI_CALL_VERSIONINFO_H

#include "aios/network/gig/multi_call/common/common.h"

namespace multi_call {

struct VersionInfo {
public:
    VersionInfo() { clear(); }
    ~VersionInfo() {}

public:
    bool complete() const { return partCount > 0 && partCount == healthCount; }
    bool heartbeatComplete() const {
        return partCount > 0 && partCount == heartbeatHealthCount;
    }
    bool subscribeComplete() const {
        return partCount > 0 && partCount == subscribeCount;
    }
    bool hasSubscribe() const { return partCount > 0 && subscribeCount > 0; }
    bool hasStopped() const { return stopCount > 0; }
    bool operator==(const VersionInfo &rhs) const {
        return partCount == rhs.partCount &&
               subscribeCount == rhs.subscribeCount &&
               stopCount == rhs.stopCount && healthCount == rhs.healthCount &&
               heartbeatHealthCount == rhs.heartbeatHealthCount &&
               minHeartbeatHealthCount == rhs.minHeartbeatHealthCount;
    }

public:
    void clear() {
        partCount = 0;
        subscribeCount = 0;
        stopCount = 0;
        healthCount = 0;
        normalProviderCount = 0;
        heartbeatHealthCount = 0;
        minHeartbeatHealthCount = 0;
    }

public:
    size_t partCount;
    size_t subscribeCount;
    size_t stopCount;
    size_t healthCount;
    size_t normalProviderCount;

    size_t heartbeatHealthCount;
    size_t minHeartbeatHealthCount;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(VersionInfo);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_VERSIONINFO_H
