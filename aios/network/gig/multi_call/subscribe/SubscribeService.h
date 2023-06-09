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
#ifndef ISEARCH_MULTI_CALL_SUBSCRIBESERVICE_H
#define ISEARCH_MULTI_CALL_SUBSCRIBESERVICE_H

#include "aios/network/gig/multi_call/common/WorkerNodeInfo.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/metric/IstioMetricReporter.h"
#include <string>
#include <vector>

namespace multi_call {

class SubscribeService {
public:
    SubscribeService() : _enabled(true) {}
    virtual ~SubscribeService() {}

private:
    SubscribeService(const SubscribeService &);
    SubscribeService &operator=(const SubscribeService &);

public:
    virtual bool init() = 0;
    virtual SubscribeType getType() = 0;
    virtual bool clusterInfoNeedUpdate() { return true; }
    virtual bool getClusterInfoMap(TopoNodeVec &topNodeVec, HeartbeatSpecVec &heartbeatSpecs) = 0;
    virtual bool addSubscribe(const std::vector<std::string> &names) {
        return false;
    }
    virtual bool deleteSubscribe(const std::vector<std::string> &names) {
        return false;
    }
    virtual bool checkSubscribeWork() { return true; }
    virtual bool stopSubscribe() { return false; }
    virtual void setEnable(bool enable) { _enabled = enable; }
    virtual bool isEnable() { return _enabled; }

private:
    bool _enabled;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(SubscribeService);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_SUBSCRIBESERVICE_H
