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
#ifndef ISEARCH_MULTI_CALL_REPLICACONTROLLER_H
#define ISEARCH_MULTI_CALL_REPLICACONTROLLER_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/service/SearchServiceProvider.h"

namespace multi_call {

struct SnapshotBizInfo;

class ReplicaController {
public:
    ReplicaController(const SearchServiceProviderVector *serviceVector);
    ~ReplicaController();

private:
    ReplicaController(const ReplicaController &);
    ReplicaController &operator=(const ReplicaController &);

public:
    void init(size_t bestChainQueueSize, bool multiVersion);
    void update(ControllerChain *candidate, const MetricLimits &metricLimits);
    ControllerChain *getBestChain() const;
    float getAvgWeight() const;
    void fillReplicaControllerInfo(ControllerFeedBack &feedBack) const;
    bool getBaseErrorRatio(float &baseErrorRatio) const;
    bool getBaseDegradeRatio(float &baseDegradeRatio) const;
    bool getBaseAvgLatency(float &baseAvgLatency) const;
    void fillControllerInfo(SnapshotBizInfo &bizInfo) const;
    void toString(std::string &debugStr) const;

private:
    void updateBestChain(ControllerChain *candidate,
                         const MetricLimits &metricLimits);
    void setBestChain(ControllerChain *bestChain);
    void updateReplicaStat(int64_t currentTime);
    void updateWeightStat();
    void updateGlobalAvgLatency();

private:
    const SearchServiceProviderVector *_serviceVector;
    std::vector<ControllerChain *> _bestChainQueue;
    ControllerChain *volatile _bestChain;
    volatile bool _multiVersion;
    mutable autil::ReadWriteLock _lock;
    int64_t _lastUpateTime;
    volatile float _avgWeight;
    volatile float _maxWeight;
    volatile float _globalAvgLatency;
    Filter _bestLoadBalanceLatencyFilter;
    Filter _bestLoadBalanceDegradeRatioFilter;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(ReplicaController);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_REPLICACONTROLLER_H
