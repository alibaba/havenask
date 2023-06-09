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
#ifndef MULTI_CALL_SUBSCRIBESERVICEMANAGER_H
#define MULTI_CALL_SUBSCRIBESERVICEMANAGER_H

#include "aios/network/gig/multi_call/common/WorkerNodeInfo.h"
#include "aios/network/gig/multi_call/config/SubscribeClustersConfig.h"
#include "aios/network/gig/multi_call/metric/MetricReporterManager.h"
#include "aios/network/gig/multi_call/subscribe/SubscribeServiceFactory.h"
#include "autil/Lock.h"

namespace multi_call {

class SubscribeServiceManager {

public:
    SubscribeServiceManager();
    virtual ~SubscribeServiceManager();

private:
    SubscribeServiceManager(const SubscribeServiceManager &);
    SubscribeServiceManager &operator=(const SubscribeServiceManager &);

public:
    virtual bool empty() { return _subServiceVec.empty(); };
    bool clusterInfoNeedUpdate();
    virtual bool getClusterInfoMap(TopoNodeVec &topoNodeVec, HeartbeatSpecVec &heartbeatSpecs);
    bool addSubscribe(const SubscribeClustersConfig &gigSubConf);
    bool deleteSubscribe(const SubscribeClustersConfig &gigSubConf);
    virtual bool addSubscribeService(const SubscribeConfig &config);
    virtual void enableSubscribeService(SubscribeType ssType);
    virtual void disableSubscribeService(SubscribeType ssType);
    virtual void stopSubscribeService(SubscribeType ssType);
    virtual bool subscribeBiz(const std::string &bizName);
    virtual bool unsubscribeBiz(const std::string &bizName);

public:
    void setMetricReporterManager(
        const MetricReporterManagerPtr &metricReporterManager) {
        _metricReporterManager = metricReporterManager;
    }

private:
    MetricReporterManagerPtr _metricReporterManager;
    autil::ReadWriteLock _subLock;
    std::vector<SubscribeServicePtr> _subServiceVec;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(SubscribeServiceManager);

} // namespace multi_call

#endif // MULTI_CALL_SUBSCRIBESERVICEMANAGER_H
