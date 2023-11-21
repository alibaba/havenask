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

#include <functional>
#include <map>
#include <set>
#include <string>

#include "autil/RangeUtil.h"
#include "autil/legacy/jsonizable.h"
#include "autil/result/Result.h"
#include "fslib/fs/FileSystem.h"
#include "suez/admin/ClusterService.pb.h"
#include "suez/common/InnerDef.h"
#include "suez/sdk/BizMeta.h"
#include "suez/sdk/JsonNodeRef.h"
#include "suez/sdk/ServiceInfo.h"

namespace catalog {
class Table;
class Partition;
struct PartitionId;
class LoadStrategy;
class TableGroup;
} // namespace catalog

namespace catalog {
namespace proto {
class Build;
}
} // namespace catalog

namespace suez {

struct PartitionInfo final : public autil::legacy::Jsonizable {
    PartitionInfo();

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    bool operator==(const PartitionInfo &other) const;
    bool operator!=(const PartitionInfo &other) const;

    uint32_t rangeFrom;
    uint32_t rangeTo;
    int32_t incVersion;
    uint32_t schemaVersion;
    uint64_t indexSize;
};
using PartitionInfoMap = std::map<std::string, PartitionInfo>;

struct GenerationInfo final : public autil::legacy::Jsonizable {
    GenerationInfo();

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    bool operator==(const GenerationInfo &other) const;
    bool operator!=(const GenerationInfo &other) const;

    uint32_t getFullVersion() const { return generationId; }
    size_t getPartitionCount() const { return partitions.size(); }
    const std::string &getIndexRoot() const { return !rawIndexRoot.empty() ? rawIndexRoot : indexRoot; }
    uint64_t getIndexSize() const;

    bool fill(const std::string &rootPath,
              const std::string &tempaltePath,
              const catalog::Table *table,
              const catalog::Partition *part,
              const std::map<catalog::PartitionId, std::map<uint32_t, catalog::proto::Build>> &builds,
              const catalog::LoadStrategy *loadStrategy);
    bool uploadFile(const std::string &srcPath, const std::string &content);
    bool genPartitions(int32_t shardCount, int32_t roleId = 1, int32_t roleCount = 1);
    bool splitPartitions(const PartitionInfoMap &oldPartitions, int32_t shardCount, int32_t roleId, int32_t roleCount);
    bool uploadSchema(const std::string &indexRoot, const catalog::Table *table);
    bool getLatestReadyBuild(const std::map<uint32_t, catalog::proto::Build> &partBuild,
                             catalog::proto::Build &latestBuild);

    uint32_t generationId;
    std::string configPath;
    std::string indexRoot;
    std::string rawIndexRoot;
    int32_t mode;
    PartitionInfoMap partitions;
    int32_t totalPartitionCount;
};

using TableInfo = std::map<std::string, GenerationInfo>; // generation_id -> GenerationInfo
using TableInfos = std::map<std::string, TableInfo>;     // table_name -> TableInfo

struct ZoneTarget final : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    bool fillZoneInfo(const Cluster &cluster);
    bool fillBizInfo(const ClusterDeployment &deployment,
                     const std::string &templatePath,
                     const std::string &storeRoot,
                     std::string &lastSignature,
                     std::string &lastBizConfigPath);
    bool fillBizClusterFiles(const std::string &templatePath, const std::vector<const catalog::Table *> tables);
    bool fillServiceInfo(const std::string &serviceInfoStr);
    bool fillTableInfos(const std::string &rootPath,
                        const std::string &templatePath,
                        const std::vector<const catalog::Table *> &tables,
                        const std::map<catalog::PartitionId, std::map<uint32_t, catalog::proto::Build>> &builds,
                        const catalog::TableGroup *tableGroup);
    autil::Result<ZoneTarget> split(int32_t idx, int32_t totalRoleCount) const;

    bool operator==(const ZoneTarget &other) const;
    bool operator!=(const ZoneTarget &other) const;

    std::string groupName;
    std::string hippoConfig;
    TableInfos tableInfos;
    uint32_t shardCount = 0;
    BizMetas bizMetas;
    ServiceInfo serviceInfo;
};

struct AdminTarget final : public autil::legacy::Jsonizable {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    ZoneTarget splitZoneTarget(int32_t idx, int32_t totalRoleCount);

    bool parseFrom(const JsonNodeRef::JsonMap &jsonMap);
    JsonNodeRef::JsonMap toJsonMap() const;

    bool operator==(const AdminTarget &other) const;
    bool operator!=(const AdminTarget &other) const;

    std::map<std::string, ZoneTarget> zones;
};

} // namespace suez
