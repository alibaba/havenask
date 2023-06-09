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
#ifndef ISEARCH_MULTI_CALL_HEARTBEATCLIENT_H
#define ISEARCH_MULTI_CALL_HEARTBEATCLIENT_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/heartbeat/HeartbeatArpc.h"
#include "aios/network/gig/multi_call/heartbeat/HeartbeatGrpc.h"
#include "aios/network/gig/multi_call/heartbeat/HeartbeatStubPool.h"
#include "aios/network/gig/multi_call/heartbeat/MetaManagerClient.h"
#include "autil/ThreadPool.h"
#include <unordered_set>
#include <vector>

namespace multi_call {

class HeartbeatClient : public std::enable_shared_from_this<HeartbeatClient> {
public:
    HeartbeatClient(const ConnectionManagerPtr &cm, const MiscConfigPtr &cfg);
    ~HeartbeatClient();

private:
    HeartbeatClient(const HeartbeatClient &);
    HeartbeatClient &operator=(const HeartbeatClient &);

public:
    bool init();
    // a server service may be published with multiple biz names, so param
    // providerVec is vector hb request only once, but should update infos for
    // all bizs in providerVec
    void heartbeat(const std::string &hbSpec,
                   std::vector<SearchServiceProviderPtr> &providerVec);
    void fillHbRequest(const std::string &hbSpec, HeartbeatRequest &hbRequest);
    void processHbResponse(const std::string &hbSpec,
                           std::vector<SearchServiceProviderPtr> &providerVec,
                           HeartbeatResponse &hbResponse);
    bool clearObsoleteSpec(const std::unordered_set<std::string> &allSpecSet);
    void fillHbMetaInTopoNode(TopoNodeVec &topoNodeVec);
    void setHeartbeatMetricReporterPtr(
        const HeartbeatMetricReporterPtr &heartbeatMetricReporter) {
        _heartbeatMetricReporter = heartbeatMetricReporter;
    }
    const HeartbeatMetricReporterPtr &getHeartbeatMetricReporterPtr() {
        return _heartbeatMetricReporter;
    }

public:
    autil::ThreadPoolPtr &getHbThreadPool() { return _hbThreadPool; }
    HeartbeatStubPoolPtr &getHbStubPool() { return _stubPool; }
    MetaManagerClientPtr &getMetaManagerClient() { return _metaManagerClient; }

private:
    const MiscConfigPtr _miscConfig;
    MetaManagerClientPtr _metaManagerClient;
    autil::ThreadPoolPtr _hbThreadPool;
    HeartbeatStubPoolPtr _stubPool;
    HeartbeatMetricReporterPtr _heartbeatMetricReporter;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(HeartbeatClient);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_HEARTBEATCLIENT_H
