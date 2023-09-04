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

#include "aios/network/gig/multi_call/common/WorkerNodeInfo.h"
#include "aios/network/gig/multi_call/proto/NewHeartbeat.pb.h"

namespace multi_call {

class SearchServiceProvider;
class SearchServiceReplica;
class HostHeartbeatStats;

class ClientTopoInfo
{
public:
    ClientTopoInfo(const std::shared_ptr<HostHeartbeatStats> &hostStats);
    ~ClientTopoInfo();

public:
    bool init(const std::string &clusterName, bool enableClusterBizSearch, const Spec &spec,
              const GigMetaEnv &envDef, const BizTopoDef &topoDef, int64_t netLatencyUs);
    TopoNode &getTopoNode() {
        return _topoNode;
    }
    SignatureTy getPublishId() const {
        return _signature.publishId;
    }
    const std::shared_ptr<HostHeartbeatStats> &getHostStats() const {
        return _hostStats;
    }
    void update(const BizTopoDef &bizTopoDef, int64_t netLatencyUs);
    std::shared_ptr<SearchServiceProvider> getProvider() const;
    void bind(const std::shared_ptr<SearchServiceReplica> &replica,
              const std::shared_ptr<SearchServiceProvider> &provider);
    void unBind();
    void fillTopoRequest(TopoHeartbeatRequest &request) const;
    void disableProvider() const;
    void toString(std::string &debugStr, MetasSignatureMap &allMetas) const;

private:
    void initMetas(SignatureTy sig, const BizMetasDef &metas);
    void initTags(SignatureTy sig, const BizTagsDef &tags);
    void updatePropagationStat(const PropagationStatDef &statDef, int64_t netLatencyUs);
    void flushUpdate();

private:
    std::shared_ptr<HostHeartbeatStats> _hostStats;
    TopoNode _topoNode;
    mutable autil::ReadWriteLock _lock;
    MetaMapPtr _metas;
    TagMapPtr _tags;
    BizTopoSignature _signature;
    bool _hasServerAgent;
    PropagationStatDef _firstPropagationStatDef;
    int64_t _initNetLatencyUs;
    std::shared_ptr<SearchServiceProvider> _provider;
    std::shared_ptr<SearchServiceReplica> _replica;
};

MULTI_CALL_TYPEDEF_PTR(ClientTopoInfo);

class ClientTopoInfoMap;
MULTI_CALL_TYPEDEF_PTR(ClientTopoInfoMap);

class ServerId;

class ClientTopoInfoMap
{
public:
    ClientTopoInfoMap(uint64_t clientId);
    ~ClientTopoInfoMap();

public:
    bool init(const std::string &ip, const ClientTopoInfoMapPtr &oldMap,
              const NewHeartbeatResponse &response);
    ClientTopoInfoPtr getInfo(SignatureTy topoSig) const;
    void addInfo(int64_t topoSig, const ClientTopoInfoPtr &info);
    bool update(const NewHeartbeatResponse &response, int64_t netLatencyUs);
    void fillRequest(NewHeartbeatRequest &request) const;
    bool fillBizInfoMap(BizInfoMap &bizInfoMap) const;
    void disableAllProvider() const;
    void toString(std::string &debugStr, MetasSignatureMap &allMetas) const;
    const std::shared_ptr<ServerId> &getServerId() const {
        return _serverId;
    }

private:
    uint64_t _clientId;
    std::shared_ptr<ServerId> _serverId;
    std::map<SignatureTy, ClientTopoInfoPtr> _topoMap;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace multi_call
