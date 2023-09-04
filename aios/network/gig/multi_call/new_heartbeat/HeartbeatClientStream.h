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

#include "aios/network/gig/multi_call/new_heartbeat/ClientTopoInfoMap.h"
#include "aios/network/gig/multi_call/stream/GigClientStream.h"

namespace multi_call {

class HeartbeatClientManagerNotifier;
class HostHeartbeatStats;

class HeartbeatClientStream : public GigClientStream
{
public:
    HeartbeatClientStream(const std::shared_ptr<HostHeartbeatStats> &hostStats,
                          const ClientTopoInfoMapPtr &clientInfoMap, const std::string &clusterName,
                          bool enableClusterBizSearch);
    ~HeartbeatClientStream();

private:
    HeartbeatClientStream(const HeartbeatClientStream &);
    HeartbeatClientStream &operator=(const HeartbeatClientStream &);

public:
    google::protobuf::Message *newReceiveMessage(google::protobuf::Arena *arena) const override;
    bool receive(const GigStreamMessage &message) override;
    void receiveCancel(const GigStreamMessage &message, MultiCallErrorCode ec) override;
    void notifyIdle(PartIdTy partId) override;

public:
    bool tick();
    ClientTopoInfoMapPtr getClientMap() const;

private:
    void fillRequest(NewHeartbeatRequest &request);
    bool doReceive(const NewHeartbeatResponse &response, int64_t netLatencyUs);
    void setClientMap(const ClientTopoInfoMapPtr &newMap);
    ClientTopoInfoPtr getOldInfo(const ClientTopoInfoMapPtr &clientMap, SignatureTy topoSig) const;

private:
    std::shared_ptr<HostHeartbeatStats> _hostStats;
    std::shared_ptr<HeartbeatClientManagerNotifier> _notifier;
    mutable autil::ReadWriteLock _clientMapLock;
    ClientTopoInfoMapPtr _clientInfoMap;
    std::string _clusterName;
    bool _enableClusterBizSearch;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(HeartbeatClientStream);

} // namespace multi_call
