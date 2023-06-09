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
#ifndef ISEARCH_MULTI_CALL_HEARTBEATWORKITEM_H
#define ISEARCH_MULTI_CALL_HEARTBEATWORKITEM_H
#include "aios/network/arpc/arpc/ANetRPCController.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/metric/MetricReporterManager.h"
#include "aios/network/gig/multi_call/proto/Heartbeat.pb.h"
#include "autil/ThreadPool.h"
#include "autil/WorkItem.h"
#include "google/protobuf/arena.h"
#include <grpc++/grpc++.h>
#include <string>
#include <vector>

namespace multi_call {

struct HeartbeatWorkerArgs;
MULTI_CALL_TYPEDEF_PTR(HeartbeatWorkerArgs);

class HeartbeatWorkItem : public autil::WorkItem {
public:
    explicit HeartbeatWorkItem(const HeartbeatWorkerArgsPtr &args)
        : _statusCode(GIG_UNKNOWN), _args(args), _isFailed(false) {
        _requestSendTime = autil::TimeUtility::currentTime();
    }
    void setStatusCode(const std::string &statusCode) {
        _statusCode = statusCode;
    }
    void setResponseRecvTime() {
        _responseRecvTime = autil::TimeUtility::currentTime();
    }
    void setFailed(bool isFailed) { _isFailed = isFailed; }
    void callback();

public:
    void process() override;

private:
    // "ARPC_" + arpc::ErrorCode | "GRPC_" + grpc::StatusCode
    std::string _statusCode;
    int64_t _requestSendTime;
    int64_t _responseRecvTime;
    HeartbeatWorkerArgsPtr _args;
    bool _isFailed;
};

class HeartbeatClient;
MULTI_CALL_TYPEDEF_PTR(HeartbeatClient);
class SearchServiceProvider;
MULTI_CALL_TYPEDEF_PTR(SearchServiceProvider);

struct HeartbeatWorkerArgs {
public:
    HeartbeatWorkerArgs(const std::string &hbSpec, HeartbeatClientPtr &client,
                        std::vector<SearchServiceProviderPtr> &providerVec)
        : hbSpec(hbSpec), hbClient(client), providerVec(providerVec) {
        hbRequest =
            google::protobuf::Arena::CreateMessage<HeartbeatRequest>(&arena);
        hbResponse =
            google::protobuf::Arena::CreateMessage<HeartbeatResponse>(&arena);
    }

public:
    std::string hbSpec;
    HeartbeatClientPtr hbClient;
    std::vector<SearchServiceProviderPtr> providerVec;
    google::protobuf::Arena arena;
    HeartbeatRequest *hbRequest;
    HeartbeatResponse *hbResponse;
};

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_HEARTBEATWORKITEM_H
