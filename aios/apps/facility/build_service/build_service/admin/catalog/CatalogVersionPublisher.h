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
#include <CatalogEntity.pb.h>
#include <map>
#include <memory>
#include <stdint.h>

#include "build_service/admin/catalog/CatalogPartitionIdentifier.h"
#include "build_service/util/Log.h"

namespace catalog::proto {
class UpdateBuildRequest;
} // namespace catalog::proto

namespace build_service::proto {
class GenerationInfo;
}

namespace build_service::common {
class RpcChannelManager;
}

namespace build_service::admin {

class CatalogVersionPublisher
{
public:
    CatalogVersionPublisher(const std::shared_ptr<CatalogPartitionIdentifier>& identifier, uint32_t generationId,
                            const std::shared_ptr<common::RpcChannelManager>& rpcChannalManager);
    virtual ~CatalogVersionPublisher() = default;

public:
    void publish(const proto::GenerationInfo& generationInfo);

private:
    catalog::proto::Build getBuildInfo(const proto::GenerationInfo& generationInfo);
    bool hasDiff(const catalog::proto::Build& curBuild);
    void updatePublishedShards(catalog::proto::Build curBuild);
    // virtual for test
    virtual bool updateCatalog(const catalog::proto::UpdateBuildRequest& request);
    static bool rangeCompare(const catalog::proto::Shard::Range& lhs, const catalog::proto::Shard::Range& rhs);

private:
    using RangeComparator = bool (*)(const catalog::proto::Shard::Range&, const catalog::proto::Shard::Range&);

private:
    std::shared_ptr<CatalogPartitionIdentifier> _identifier;
    uint32_t _generationId = 0;
    std::map<catalog::proto::Shard::Range, catalog::proto::Shard, RangeComparator> _publishedShards;
    std::shared_ptr<common::RpcChannelManager> _rpcChannelManager;

private:
    BS_LOG_DECLARE();
};

} // namespace build_service::admin
