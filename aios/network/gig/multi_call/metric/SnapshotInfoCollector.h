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
#ifndef ISEARCH_MULTI_CALL_SNAPSHOTINFOCOLLECTOR_H
#define ISEARCH_MULTI_CALL_SNAPSHOTINFOCOLLECTOR_H

#include <limits>

#include "aios/network/gig/multi_call/common/ControllerParam.h"
#include "aios/network/gig/multi_call/common/common.h"

namespace multi_call {

struct SnapshotBizInfo {
    SnapshotBizInfo()
        : versionCount(0)
        , completeVersionCount(0)
        , copyVersionCount(0)
        , totalVersionWeight(0)
        , completeVersionWeight(0)
        , providerCount(0)
        , healthProviderCount(0)
        , unhealthProviderCount(0)
        , copyProviderCount(0)
        , minProviderLatency(std::numeric_limits<float>::max())
        , minErrorRatio(std::numeric_limits<float>::max())
        , minDegradeRatio(std::numeric_limits<float>::max())
        , minLoadBalanceLatency(std::numeric_limits<float>::max())
        , minLoadBalanceDegradeRatio(std::numeric_limits<float>::max())
        , avgWeight(MAX_WEIGHT_FLOAT)
        , delaySum(0)
        , delayCount(0) {
    }
    int32_t versionCount;
    int32_t completeVersionCount;
    int32_t copyVersionCount;
    int32_t totalVersionWeight;
    int32_t completeVersionWeight;

    int32_t providerCount;
    int32_t healthProviderCount;
    int32_t unhealthProviderCount;
    int32_t copyProviderCount;

    float minProviderLatency;
    float minErrorRatio;
    float minDegradeRatio;
    float minLoadBalanceLatency;
    float minLoadBalanceDegradeRatio;
    float avgWeight;

    int64_t delaySum;
    int64_t delayCount;
};

typedef std::map<std::string, SnapshotBizInfo> SnapshotBizInfoMap;

class SnapshotInfoCollector
{
public:
    SnapshotInfoCollector();
    ~SnapshotInfoCollector();

private:
    SnapshotInfoCollector(const SnapshotInfoCollector &);
    SnapshotInfoCollector &operator=(const SnapshotInfoCollector &);

public:
    void addSnapshotBizInfo(const std::string &bizName, const SnapshotBizInfo &snapshotInfo);
    const SnapshotBizInfoMap &getSnapshotInfoMap() const;

private:
    SnapshotBizInfoMap _snapshotInfoMap;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_SNAPSHOTINFOCOLLECTOR_H
