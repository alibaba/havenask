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

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/proto/NewHeartbeat.pb.h"
#include "aios/network/gig/multi_call/common/MetaEnv.h"
#include "aios/network/gig/multi_call/common/Spec.h"
#include "aios/network/gig/multi_call/new_heartbeat/BizTopo.h"

namespace multi_call {

class TopoInfoBuilder;

class ServerId {
public:
    void init(const Spec &spec);
    void fillServerIdDef(const SignatureMapPtr &clientSignatureMap, NewHeartbeatResponse &response) const;
    void fromProto(SignatureTy signature, const std::string &ip, const ServerIdDef &def);
    const Spec &getSpec() const {
        return _spec;
    }
    void toString(std::string &ret) const;
    SignatureTy getSignature() const {
        return _signature;
    }
private:
    void initSignature();
private:
    MetaEnv _metaEnv;
    Spec _spec;
    SignatureTy _signature;
private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(ServerId);

class ServerTopoMap
{
public:
    ServerTopoMap();
    ~ServerTopoMap();
private:
    ServerTopoMap(const ServerTopoMap &);
    ServerTopoMap &operator=(const ServerTopoMap &);
public:
    bool setSpec(const Spec &spec);
    std::string getTopoInfoStr() const;
    void setServerId(const ServerIdPtr &serverId);
    ServerIdPtr getServerId() const;
    void setBizTopoMap(const BizTopoMapPtr &newMap);
    BizTopoMapPtr getBizTopoMap() const;
    std::string toString() const;
private:
    mutable autil::ReadWriteLock _lock;
    ServerIdPtr _serverId;
    BizTopoMapPtr _bizTopoMap;
private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(ServerTopoMap);

}
