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
#include "aios/network/gig/multi_call/interface/Response.h"

#include "aios/network/gig/multi_call/proto/GigAgent.pb.h"

namespace multi_call {

Response::~Response() {
    freeProtoMessage(_agentInfo);
}

void Response::initAgentInfo(const std::string &agentInfoStr) {
    if (agentInfoStr.empty()) {
        return;
    }
    GigResponseInfo *responseInfo = NULL;
    if (_arena) {
        responseInfo = google::protobuf::Arena::CreateMessage<GigResponseInfo>(_arena.get());
    } else {
        responseInfo = new GigResponseInfo();
    }
    std::unique_ptr<GigResponseInfo, void (*)(google::protobuf::Message *)> pbInfo(
        responseInfo, freeProtoMessage);
    auto &info = *pbInfo;
    if (!info.ParseFromString(agentInfoStr)) {
        return;
    }
    freeProtoMessage(_agentInfo);
    _agentInfo = pbInfo.release();
}

bool Response::hasStatInAgentInfo() {
    if (!_agentInfo) {
        return false;
    }
    return _agentInfo->has_error_ratio() || _agentInfo->has_degrade_ratio() ||
           _agentInfo->has_avg_latency() || _agentInfo->has_target_weight() ||
           _agentInfo->has_warm_up_status() || _agentInfo->has_propagation_stats();
}

} // namespace multi_call