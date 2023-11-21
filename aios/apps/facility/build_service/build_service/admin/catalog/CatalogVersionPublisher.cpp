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
#include "build_service/admin/catalog/CatalogVersionPublisher.h"

#include <arpc/ANetRPCController.h>
#include <google/protobuf/service.h>
#include <stddef.h>
#include <string>
#include <utility>

#include "alog/Logger.h"
#include "build_service/common/RpcChannelManager.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "catalog/proto/CatalogService.pb.h"

namespace build_service::admin {
BS_LOG_SETUP(admin, CatalogVersionPublisher);

CatalogVersionPublisher::CatalogVersionPublisher(const std::shared_ptr<CatalogPartitionIdentifier>& identifier,
                                                 uint32_t generationId,
                                                 const std::shared_ptr<common::RpcChannelManager>& rpcChannalManager)
    : _identifier(identifier)
    , _generationId(generationId)
    , _publishedShards(rangeCompare)
    , _rpcChannelManager(rpcChannalManager)
{
}

void CatalogVersionPublisher::publish(const proto::GenerationInfo& generationInfo)
{
    auto curBuild = getBuildInfo(generationInfo);
    if (!hasDiff(curBuild)) {
        return;
    }
    catalog::proto::UpdateBuildRequest request;
    *request.mutable_build() = curBuild;
    if (!updateCatalog(request)) {
        BS_LOG(ERROR, "update catalog failed, request[%s]", request.ShortDebugString().c_str());
        return;
    }
    updatePublishedShards(std::move(curBuild));
}

catalog::proto::Build CatalogVersionPublisher::getBuildInfo(const proto::GenerationInfo& generationInfo)
{
    auto fillShard = [](const auto& indexInfo, auto shard) {
        shard->mutable_shard_range()->set_from(indexInfo.range().from());
        shard->mutable_shard_range()->set_to(indexInfo.range().to());
        shard->set_index_version(indexInfo.indexversion());
        shard->set_index_version_timestamp(indexInfo.versiontimestamp());
    };
    catalog::proto::Build build;
    auto buildId = build.mutable_build_id();
    buildId->set_generation_id(_generationId);
    buildId->set_partition_name(_identifier->getPartitionName());
    buildId->set_table_name(_identifier->getTableName());
    buildId->set_database_name(_identifier->getDatabaseName());
    buildId->set_catalog_name(_identifier->getCatalogName());
    build.mutable_current()->set_index_root(generationInfo.indexroot());
    for (size_t i = 0; i < generationInfo.indexinfos_size(); ++i) {
        const auto& indexInfo = generationInfo.indexinfos(i);
        // only collect valid version
        if (indexInfo.versiontimestamp() <= 0) {
            continue;
        }
        auto shard = build.mutable_current()->add_shards();
        fillShard(indexInfo, shard);
    }
    if (build.current().shards_size() > 0) {
        return build;
    }
    for (size_t i = 0; i < generationInfo.fullindexinfos_size(); ++i) {
        const auto& indexInfo = generationInfo.fullindexinfos(i);
        // only collect valid version
        if (indexInfo.versiontimestamp() <= 0) {
            continue;
        }
        auto shard = build.mutable_current()->add_shards();
        fillShard(indexInfo, shard);
    }
    return build;
}

bool CatalogVersionPublisher::hasDiff(const catalog::proto::Build& curBuild)
{
    for (int i = 0; i < curBuild.current().shards_size(); ++i) {
        const auto& curShard = curBuild.current().shards(i);
        const auto& shardRange = curShard.shard_range();
        auto iter = _publishedShards.find(shardRange);
        if (iter == _publishedShards.end()) {
            return true;
        }
        const auto& publishedShard = iter->second;
        if (curShard.index_version() != publishedShard.index_version() ||
            curShard.index_version_timestamp() != publishedShard.index_version_timestamp()) {
            return true;
        }
    }
    return false;
}

void CatalogVersionPublisher::updatePublishedShards(catalog::proto::Build curBuild)
{
    _publishedShards.clear();
    for (int i = 0; i < curBuild.current().shards_size(); ++i) {
        const auto& shard = curBuild.current().shards(i);
        _publishedShards[shard.shard_range()] = shard;
    }
}

bool CatalogVersionPublisher::updateCatalog(const catalog::proto::UpdateBuildRequest& request)
{
    arpc::ANetRPCController controller;
    auto rpcChannel = _rpcChannelManager->getRpcChannel();
    if (!rpcChannel) {
        BS_LOG(ERROR, "get rpc channel failed");
        return false;
    }

    catalog::proto::CatalogService_Stub stub(rpcChannel.get(), google::protobuf::Service::STUB_DOESNT_OWN_CHANNEL);
    const int RPC_TIMEOUT = 30 * 1000; // 30s
    controller.SetExpireTime(RPC_TIMEOUT);
    catalog::proto::BuildCommonResponse response;
    stub.updateBuild(&controller, &request, &response, NULL);
    if (controller.Failed()) {
        BS_LOG(ERROR, "rpc updateBuild failed, ec:[%d] error:[%s]", controller.GetErrorCode(),
               controller.ErrorText().c_str());
        return false;
    }
    if (response.status().code() != catalog::proto::ResponseStatus::OK) {
        BS_LOG(ERROR, "rpc updateBuild failed, response[%s]", response.ShortDebugString().c_str());
        return false;
    }
    return true;
}

bool CatalogVersionPublisher::rangeCompare(const catalog::proto::Shard::Range& lhs,
                                           const catalog::proto::Shard::Range& rhs)
{
    if (lhs.from() == rhs.from()) {
        return lhs.to() < rhs.to();
    }
    return lhs.from() < rhs.from();
}

} // namespace build_service::admin
