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
#include <iostream>
#include <memory>
#include <string>
#include <unistd.h>

#include "aios/network/gig/multi_call/subscribe/GrpcAds.h"
#include "autil/HashAlgorithm.h"
#include "autil/Log.h"
#include "autil/StringTokenizer.h"
#include <grpc++/grpc++.h>
namespace multi_call {
using autil::HashAlgorithm;
using autil::StringTokenizer;
using envoy::api::v2::Cluster;
using envoy::api::v2::DiscoveryRequest;
using envoy::api::v2::DiscoveryResponse;
using envoy::api::v2::core::Node;
using envoy::service::discovery::v2::AggregatedDiscoveryService;
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;

AUTIL_LOG_SETUP(multi_call, GrpcAdsClient);
GrpcAdsClient::GrpcAdsClient(const std::string &nodeId,
                             const std::vector<std::string> &xdsServers)
    : stub_(nullptr), nodeId_(nodeId) {
    //  解析多个集群的 Xds server 信息
    std::vector<std::string> serversVec;
    StringTokenizer tokenizer;
    for (auto serversStr : xdsServers) {
        serversVec.clear();
        tokenizer.tokenize(serversStr, ",");
        auto serverStrVec = tokenizer.getTokenVector();
        for (auto serverStr : serverStrVec) {
            serversVec.emplace_back(serverStr);
        }
        xdsClusters_.emplace_back(serversVec);
    }
    context_.reset();
}

GrpcAdsClient::~GrpcAdsClient() {}

void GrpcAdsClient::TryCancel() {
    if (context_.get())
        context_->TryCancel();
}

bool GrpcAdsClient::BuildStream() {
    if (nodeId_ == "") {
        AUTIL_LOG(ERROR, "build grpc stream failed. nodeId can't be empty.");
        return false;
    }

    grpc::ChannelArguments channelArg;
    channelArg.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 300000);
    channelArg.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 20000);
    channelArg.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
    channelArg.SetInt(GRPC_ARG_HTTP2_BDP_PROBE, 1);
    channelArg.SetInt(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS,
                      5000);
    channelArg.SetInt(GRPC_ARG_HTTP2_MIN_SENT_PING_INTERVAL_WITHOUT_DATA_MS,
                      300000);
    channelArg.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
    channelArg.SetInt(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, -1);

    uint64_t key = HashAlgorithm::hashString64(nodeId_.c_str());
    bool connectState = false;
    for (auto clusters : xdsClusters_) {
        uint32_t serverSize = clusters.size();
        uint32_t idx = key % serverSize;
        for (uint32_t i = 0; i < serverSize; i++) {
            autil::ScopedLock lock(_channelMutex);
            {
                channel_.reset();
                channel_ = grpc::CreateCustomChannel(
                    clusters[idx], grpc::InsecureChannelCredentials(),
                    channelArg);
            }
            if (nullptr == channel_) {
                AUTIL_LOG(WARN, "create channel failed. server:%s",
                          clusters[idx].c_str());
                idx = (idx + i + 1) % serverSize;
                continue;
            }
            // check connecting, retry 20 times
            for (auto i = 0; i < 20; i++) {
                if (GRPC_CHANNEL_READY != channel_->GetState(true)) {
                    AUTIL_LOG(INFO,
                              "channel not ready, retry time:%d. server:%s", i,
                              clusters[idx].c_str());
                    usleep(500000);
                    continue;
                }
                connectState = true;
                AUTIL_LOG(INFO, "channel connect success. server:%s",
                          clusters[idx].c_str());
                break;
            }
            if (false == connectState) {
                AUTIL_LOG(ERROR, "channel connect failed. server:%s",
                          clusters[idx].c_str());
                continue;
            }
            AUTIL_LOG(INFO, "connect XDS Server success. server:%s",
                      clusters[idx].c_str());
            break;
        } // end for (auto i=0; i<serverSize; i++)
        if (false == connectState) {
            AUTIL_LOG(ERROR,
                      "failed to connect this cluster. try backup cluster.");
            continue;
        }
        break;
    } // end for (auto clusters : xdsClusters_) {
    if (false == connectState) {
        AUTIL_LOG(ERROR, "try to connect all clusters, but failed.");
        return false;
    }
    stub_.reset();
    stub_ = AggregatedDiscoveryService::NewStub(channel_);
    if (nullptr == stub_) {
        AUTIL_LOG(ERROR,
                  "create grpc ADS stub failed on channel. return false");
        return false;
    }
    context_.reset();
    context_ = make_unique<ClientContext>();
    adsStream_ = stub_->StreamAggregatedResources(context_.get());
    if (nullptr == adsStream_) {
        AUTIL_LOG(ERROR, "create ads grpc stream failed. return false");
        context_.reset();
        return false;
    }
    AUTIL_LOG(INFO, "build grpc ads stream success.");
    return true;
}

bool GrpcAdsClient::SendDiscoveryRequest(
    const std::string &typeUrl, const std::set<std::string> &resourceNames) {
    autil::ScopedReadWriteLock lock(rwlock_, 'w');
    DiscoveryRequest discReq;
    discReq.set_type_url(typeUrl);
    for (const auto &resource : resourceNames) {
        discReq.add_resource_names(resource);
    }
    discReq.mutable_node()->set_id(nodeId_);
    if (nullptr == adsStream_) {
        return false;
    }
    AUTIL_LOG(DEBUG, "SendDiscoveryRequest [%s]",
              discReq.DebugString().c_str());
    adsStream_->Write(discReq);
    return true;
}

const std::string GrpcAdsClient::getChannelState() {
    std::string ret = "GRPC_CHANNEL_INVALID";
    autil::ScopedLock lock(_channelMutex);
    {
        if (channel_) {
            grpc_connectivity_state status = channel_->GetState(false);
            switch (status) {
            case GRPC_CHANNEL_IDLE:
                ret = "GRPC_CHANNEL_IDLE";
                break;
            case GRPC_CHANNEL_CONNECTING:
                ret = "GRPC_CHANNEL_CONNECTING";
                break;
            case GRPC_CHANNEL_READY:
                ret = "GRPC_CHANNEL_READY";
                break;
            case GRPC_CHANNEL_TRANSIENT_FAILURE:
                ret = "GRPC_CHANNEL_TRANSIENT_FAILURE";
                break;
            case GRPC_CHANNEL_SHUTDOWN:
                ret = "GRPC_CHANNEL_SHUTDOWN";
                break;
            default:
                break;
            }
        }
    }
    return ret;
}

bool GrpcAdsClient::SendACK(const std::string &typeUrl,
                            const std::string &responseNonce) {
    autil::ScopedReadWriteLock lock(rwlock_, 'w');
    DiscoveryRequest ackReq;
    ackReq.set_type_url(typeUrl);
    ackReq.set_response_nonce(responseNonce);
    ackReq.mutable_node()->set_id(nodeId_);
    if (nullptr == adsStream_) {
        return false;
    }
    AUTIL_LOG(DEBUG, "SendACK [%s]", ackReq.DebugString().c_str());
    adsStream_->Write(ackReq);
    return true;
}

bool GrpcAdsClient::SendNACK(const std::string &typeUrl,
                             const std::string &responseNonce,
                             const std::string &failedReason) {
    autil::ScopedReadWriteLock lock(rwlock_, 'w');
    DiscoveryRequest nackReq;
    nackReq.set_type_url(typeUrl);
    nackReq.set_response_nonce(responseNonce);
    nackReq.mutable_node()->set_id(nodeId_);
    ::google::rpc::Status *error_detail = nackReq.mutable_error_detail();
    error_detail->set_code(GRPC_STATUS_FAILED_PRECONDITION);
    error_detail->set_message(failedReason);
    if (nullptr == adsStream_) {
        return false;
    }
    AUTIL_LOG(DEBUG, "SendNACK [%s]", nackReq.DebugString().c_str());
    adsStream_->Write(nackReq);
    return true;
}

DiscoveryResponsePtr GrpcAdsClient::ReadDiscoveryResponse() {
    DiscoveryResponsePtr resp = std::make_shared<DiscoveryResponse>();
    if (nullptr == adsStream_) {
        return nullptr;
    }
    auto ret = adsStream_->Read(resp.get());
    if (false == ret) {
        return nullptr;
    }
    return resp;
};

} // namespace multi_call
