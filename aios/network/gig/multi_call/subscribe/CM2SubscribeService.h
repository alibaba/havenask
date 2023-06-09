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
#ifndef ISEARCH_MULTI_CALL_CM2SUBSCRIBESERVICE_H
#define ISEARCH_MULTI_CALL_CM2SUBSCRIBESERVICE_H

#include "aios/apps/facility/cm2/cm_sub/cm_sub.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "aios/network/gig/multi_call/subscribe/SubscribeService.h"
#include "autil/Lock.h"
#include <unordered_set>

namespace cm_basic {
class CMCentralSub;
}

namespace multi_call {

typedef std::shared_ptr<cm_sub::CMSubscriber> CMSubscriberPtr;

class CM2SubscribeService : public SubscribeService {
public:
    CM2SubscribeService(const Cm2Config &config);
    ~CM2SubscribeService();

private:
    CM2SubscribeService(const CM2SubscribeService &);
    CM2SubscribeService &operator=(const CM2SubscribeService &);

public:
    bool init() override;
    SubscribeType getType() override { return ST_CM2; }
    bool clusterInfoNeedUpdate() override;
    bool getClusterInfoMap(TopoNodeVec &topoNodeVec, HeartbeatSpecVec &heartbeatSpecs) override;
    bool addSubscribe(const std::vector<std::string> &names) override;
    bool deleteSubscribe(const std::vector<std::string> &names) override;
    void addTopoCluster(const std::vector<std::string> &names);
    void deleteTopoCluster(const std::vector<std::string> &names);

private:
    bool initGroupSub();
    bool initPartSub();
    virtual CMSubscriberPtr
    createSubscriber(cm_sub::SubscriberConfig *subConfig);
    bool addClusters(cm_basic::CMCentralSub *central, TopoNodeVec &topoNodeVec,
                     HeartbeatSpecVec &heartbeatSpecs);
    bool checkClusters(cm_basic::CMCentralSub *central);

private:
    static cm_sub::SubscriberConfig *newSubConfig(const Cm2Config &config);
    static void addCluster(const std::vector<cm_basic::CMNode *> &cmNodeVec,
                           const std::string &clusterName, bool ignoreTopo,
                           bool enableClusterBizSearch, VersionTy subProtocolVersion,
                           TopoNodeVec &topoNodeVec, HeartbeatSpecVec &heartbeatSpecs);
    static void addTopoNode(cm_basic::CMNode *node,
                            const std::string &clusterName,
                            TopoNodeVec &topoNodeVec);
    static void addTopoNodeFromTopoStr(cm_basic::CMNode *node, const std::string &clusterName,
                                       bool enableClusterBizSearch, VersionTy subProtocolVersion,
                                       TopoNodeVec &topoNodeVec, HeartbeatSpecVec &heartbeatSpecs);
    static WeightTy getNodeWeight(cm_basic::CMNode *node);
    static void fillSpec(cm_basic::CMNode *node, Spec &spec);
    static void fillNodeMeta(cm_basic::CMNode *node, TopoNode &topoNode);
    static void freeNodeVec(const std::vector<cm_basic::CMNode *> &nodeVec);
    static bool isValidNode(cm_basic::CMNode *node);

private:
    Cm2Config _config;
    CMSubscriberPtr _groupSubscriber;
    CMSubscriberPtr _partSubscriber;
    autil::ThreadMutex _mutex;
    std::unordered_set<std::string> _topoClusterSet;
    std::unordered_map<std::string, int64_t> _clusterVersions;

private:
    static int _rdmaPortDiff;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(CM2SubscribeService);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_CM2SUBSCRIBESERVICE_H
