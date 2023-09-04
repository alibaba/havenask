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
#include <grpc++/grpc++.h>
#include <iostream>
#include <memory>
#include <string>

#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/envoy/api/v2/cds.grpc.pb.h"
#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/envoy/api/v2/eds.grpc.pb.h"
#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/envoy/service/discovery/v2/ads.grpc.pb.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "autil/Lock.h"
#include "autil/Log.h"

namespace multi_call {

using envoy::api::v2::Cluster;
using envoy::api::v2::ClusterDiscoveryService;
using envoy::api::v2::DiscoveryRequest;
using envoy::api::v2::DiscoveryResponse;
using envoy::api::v2::EndpointDiscoveryService;
using envoy::api::v2::core::Node;
using envoy::service::discovery::v2::AggregatedDiscoveryService;
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;

using AdsStreamPtr = std::shared_ptr<ClientReaderWriter<DiscoveryRequest, DiscoveryResponse>>;
using DiscoveryResponsePtr = std::shared_ptr<DiscoveryResponse>;

class GrpcAdsClient
{
public:
    GrpcAdsClient(const std::string &nodeId, const std::vector<std::string> &xdsServers);
    ~GrpcAdsClient();
    void TryCancel();
    bool BuildStream();
    bool SendDiscoveryRequest(const std::string &typeUrl,
                              const std::set<std::string> &resourceNames);
    bool SendACK(const std::string &typeUrl, const std::string &responseNonce);
    bool SendNACK(const std::string &typeUrl, const std::string &responseNonce,
                  const std::string &failedReason);
    DiscoveryResponsePtr ReadDiscoveryResponse();
    template <typename T, typename... Args>
    std::unique_ptr<T> make_unique(Args &&...args) {
        return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
    }
    const std::string getChannelState();

private:
    std::unique_ptr<AggregatedDiscoveryService::Stub> stub_;
    std::vector<std::vector<std::string>> xdsClusters_;
    AdsStreamPtr adsStream_;
    std::string nodeId_;
    std::unique_ptr<grpc::ClientContext> context_;
    autil::ReadWriteLock rwlock_;
    autil::ThreadMutex _channelMutex;
    std::shared_ptr<Channel> channel_;
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GrpcAdsClient);

} // namespace multi_call
