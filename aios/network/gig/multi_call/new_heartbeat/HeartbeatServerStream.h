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

#include "aios/network/gig/multi_call/new_heartbeat/ServerTopoMap.h"
#include "aios/network/gig/multi_call/stream/GigServerStream.h"

namespace multi_call {

class HeartbeatServerManager;
class NewHeartbeatRequest;
class NewHeartbeatResponse;

class HeartbeatServerStream : public GigServerStream
{
public:
    HeartbeatServerStream(const std::shared_ptr<HeartbeatServerManager> &manager);
    ~HeartbeatServerStream();

private:
    HeartbeatServerStream(const HeartbeatServerStream &);
    HeartbeatServerStream &operator=(const HeartbeatServerStream &);

public:
    google::protobuf::Message *newReceiveMessage(google::protobuf::Arena *arena) const override;
    bool receive(const multi_call::GigStreamMessage &message) override;
    void receiveCancel(const multi_call::GigStreamMessage &message,
                       multi_call::MultiCallErrorCode ec) override;
    void notifyIdle(multi_call::PartIdTy partId) override {
    }

public:
    uint64_t getClientId() const {
        return _clientId;
    }
    bool flush(bool sync);
    void stop();
    void fillResponse(NewHeartbeatResponse &response);
    bool cancelled() const {
        return _cancelled;
    }

private:
    void updateClientSignatureMap(const NewHeartbeatRequest &request);
    SignatureMapPtr getClientSignatureMap() const;
    void setClientSignatureMap(const SignatureMapPtr &newMap);

private:
    uint64_t _clientId;
    ServerTopoMapPtr _serverTopoMap;
    mutable autil::ThreadMutex _signatureMutex;
    SignatureMapPtr _clientSignatureMap;
    mutable autil::ThreadMutex _flushMutex;
    bool _inited    : 1;
    bool _cancelled : 1;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(HeartbeatServerStream);

} // namespace multi_call
