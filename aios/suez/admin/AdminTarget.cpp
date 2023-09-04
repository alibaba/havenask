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
#include "suez/admin/AdminTarget.h"

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "autil/result/Errors.h"
#include "build_service/config/BuildServiceConfig.h"
#include "catalog/entity/Partition.h"
#include "catalog/entity/Table.h"
#include "catalog/tools/BSConfigMaker.h"
#include "catalog/tools/TableSchemaConfig.h"
#include "suez/admin/BizConfigMaker.h"
#include "suez/admin/ClusterConfigJson.h"
#include "suez/admin/RangeParser.h"
#include "suez/common/TablePathDefine.h"
#include "suez/sdk/JsonNodeRef.h"
#include "suez/sdk/PathDefine.h"

using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace catalog;
using namespace fslib::fs;

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, AdminTarget);

PartitionInfo::PartitionInfo() : rangeFrom(0), rangeTo(0), incVersion(-1), schemaVersion(0), indexSize(0) {}

void PartitionInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("inc_version", incVersion, incVersion);
    json.Jsonize("schema_version", schemaVersion, schemaVersion);
    json.Jsonize("index_size", indexSize, indexSize);
}

bool PartitionInfo::operator==(const PartitionInfo &other) const {
    if (this == &other) {
        return true;
    }
    return rangeFrom == other.rangeFrom && rangeTo == other.rangeTo && incVersion == other.incVersion &&
           schemaVersion == other.schemaVersion && indexSize == other.indexSize;
}

bool PartitionInfo::operator!=(const PartitionInfo &other) const { return !(*this == other); }

GenerationInfo::GenerationInfo() : generationId(0), mode(0) {}

void GenerationInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("generation_id", generationId, generationId);
    json.Jsonize("config_path", configPath, configPath);
    json.Jsonize("index_root", indexRoot, indexRoot);
    json.Jsonize("raw_index_root", rawIndexRoot, rawIndexRoot);
    json.Jsonize("table_mode", mode, mode);
    json.Jsonize("partitions", partitions, partitions);
    json.Jsonize("total_partition_count", totalPartitionCount, totalPartitionCount);

    if (json.GetMode() == TO_JSON) {
        return;
    }

    // FROM_JSON
    for (auto &it : partitions) {
        auto range = RangeParser::parse(it.first);
        it.second.rangeFrom = range.first;
        it.second.rangeTo = range.second;
    }
}

bool GenerationInfo::operator==(const GenerationInfo &other) const {
    if (this == &other) {
        return true;
    }
    return generationId == other.generationId && configPath == other.configPath && indexRoot == other.indexRoot &&
           rawIndexRoot == other.rawIndexRoot && partitions == other.partitions && mode == other.mode;
}

bool GenerationInfo::operator!=(const GenerationInfo &other) const { return !(*this == other); }

uint64_t GenerationInfo::getIndexSize() const {
    uint64_t size = 0;
    for (const auto &it : partitions) {
        size += it.second.indexSize;
    }
    return size;
}

bool GenerationInfo::fill(const std::string &rootPath,
                          const std::string &templatePath,
                          const catalog::Table *table,
                          const catalog::Partition *part,
                          const std::map<catalog::PartitionId, catalog::proto::Build> &builds) {
    auto tableStructure = part->getTableStructure();
    if (tableStructure == nullptr) {
        AUTIL_LOG(ERROR, "get table structure failed");
        return false;
    }

    switch (tableStructure->buildType()) {
    case proto::BuildType::OFFLINE: {
        auto iter = builds.find(part->id());
        if (iter == builds.end()) {
            return true;
        }
        const auto &build = iter->second;
        const auto &current = build.current();
        if (current.shards_size() == 0) {
            return true;
        }
        if (current.shards_size() != part->shardCount()) {
            AUTIL_LOG(ERROR,
                      "total partition count is %d, but build shards count %d",
                      part->shardCount(),
                      current.shards_size());
            return false;
        }

        indexRoot = current.index_root();
        configPath = current.config_path();
        generationId = build.build_id().generation_id();
        mode = 1;
        if (!genPartitions(part->shardCount())) {
            return false;
        }
        for (const auto &shard : current.shards()) {
            autil::PartitionRange range{shard.shard_range().from(), shard.shard_range().to()};
            auto rangeName = autil::RangeUtil::getRangeName(range);
            if (partitions.count(rangeName) > 0) {
                partitions[rangeName].incVersion = shard.index_version();
            }
        }
        totalPartitionCount = part->shardCount();
        break;
    }
    case proto::BuildType::DIRECT: {
        auto s = catalog::BSConfigMaker::Make(*part, templatePath, rootPath, &configPath);
        if (!isOk(s)) {
            AUTIL_LOG(ERROR, "make config failed, error msg: %s", s.message().c_str());
            return false;
        }

        indexRoot = PathDefine::getIndexRoot(rootPath, table->tableName());
        mode = 1;
        if (!genPartitions(part->shardCount())) {
            return false;
        }
        // suez加载的时候会读index目录下的schema, 直写表需要先生成schema
        if (!uploadSchema(indexRoot, table)) {
            return false;
        }
        totalPartitionCount = part->shardCount();
        break;
    }
    default:
        AUTIL_LOG(ERROR, "not support build type %d", tableStructure->buildType());
        return false;
    }

    return true;
}

bool GenerationInfo::uploadSchema(const std::string &indexRoot, const catalog::Table *table) {
    TableSchemaConfig schemaConfig;
    if (!schemaConfig.init(table->tableName(), table->tableStructure().get())) {
        return false;
    }

    for (const auto &iter : partitions) {
        const auto &fileName = PathDefine::join(
            TablePathDefine::constructIndexPath(indexRoot, table->tableName(), generationId, iter.first),
            "schema.json");
        if (!uploadFile(fileName, autil::legacy::FastToJsonString(schemaConfig))) {
            return false;
        }
    }
    return true;
}

bool GenerationInfo::uploadFile(const std::string &srcPath, const std::string &content) {
    if (FileSystem::isExist(srcPath) == fslib::EC_TRUE) {
        return true;
    }

    auto result = FileSystem::writeFile(srcPath, content) == fslib::EC_OK;
    if (!result) {
        AUTIL_LOG(ERROR, "upload file %s failed", srcPath.c_str());
    }
    return result;
}

bool GenerationInfo::genPartitions(int32_t shardCount, int32_t roleId, int32_t roleCount) {
    PartitionInfoMap result;
    for (size_t i = 0; i < shardCount; i++) {
        if ((i % roleCount) != (roleId % roleCount)) {
            continue;
        }
        autil::PartitionRange range;
        if (!autil::RangeUtil::getRange(shardCount, i, range)) {
            return false;
        }
        PartitionInfo info;
        info.rangeFrom = range.first;
        info.rangeTo = range.second;
        info.incVersion = -2;
        info.schemaVersion = 0;
        result.emplace(autil::RangeUtil::getRangeName(range), info);
    }
    partitions = result;
    return true;
}

bool GenerationInfo::splitPartitions(const PartitionInfoMap &oldPartitions,
                                     int32_t shardCount,
                                     int32_t roleId,
                                     int32_t roleCount) {
    partitions.clear();
    for (size_t i = 0; i < totalPartitionCount; i++) {
        if ((i % roleCount) != (roleId % roleCount)) {
            continue;
        }
        autil::PartitionRange range;
        if (!autil::RangeUtil::getRange(shardCount, i, range)) {
            return false;
        }
        const auto &rangeName = autil::RangeUtil::getRangeName(range);
        auto gInfoIter = oldPartitions.find(rangeName);
        if (gInfoIter == oldPartitions.end()) {
            return false;
        }
        partitions.emplace(autil::RangeUtil::getRangeName(range), gInfoIter->second);
    }
    return true;
}

void ZoneTarget::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    if (json.GetMode() == FROM_JSON) {
        // fast jsonizable和legacy jsonizable在处理int64时存在不相容的问题
        const auto &jsonMap = json.GetMap();
        shardCount = getField(&jsonMap, "role_count", 0);
        json.Jsonize("hippo_config", hippoConfig, hippoConfig);
    }
    json.Jsonize("group_name", groupName, groupName);
    json.Jsonize("table_info", tableInfos, tableInfos);
    json.Jsonize("biz_info", bizMetas, bizMetas);
    json.Jsonize("service_info", serviceInfo, serviceInfo);

    if (json.GetMode() == TO_JSON) {
        json.Jsonize("role_count", shardCount);

        Any any;
        try {
            FastFromJsonString(any, hippoConfig);
        } catch (const autil::legacy::ExceptionBase &e) {
            AUTIL_LOG(WARN, "parse hippo config [%s] failed,  error[%s]", hippoConfig.c_str(), e.what());
        }
        JsonNodeRef::JsonMap *jsonMap = AnyCast<JsonNodeRef::JsonMap>(&any);
        json.Jsonize("hippo_config", jsonMap);
    }

    // FROM_JSON
    for (auto &it1 : tableInfos) {
        for (auto &it2 : it1.second) {
            auto &generationInfo = it2.second;
            // TODO: check
            autil::StringUtil::fromString(it2.first, generationInfo.generationId);
            shardCount = std::max(shardCount, (uint32_t)generationInfo.partitions.size());
        }
    }
    shardCount = shardCount > 0 ? shardCount : 1;
}

bool ZoneTarget::fillZoneInfo(const Cluster &cluster) {
    groupName = cluster.clustername();
    ClusterConfigJson config;
    try {
        autil::legacy::FastFromJsonString(config, cluster.config().configstr());
    } catch (const autil::legacy::ExceptionBase &e) {
        AUTIL_LOG(ERROR,
                  "parse cluster [%s] config json failed, config: [%s], error: [%s]",
                  cluster.clustername().c_str(),
                  cluster.config().configstr().c_str(),
                  e.what());
        return false;
    }
    hippoConfig = config.hippoConfig;
    return true;
}

bool ZoneTarget::fillBizInfo(const ClusterDeployment &deployment,
                             const std::string &templatePath,
                             const std::string &storeRoot,
                             std::string &lastSignature,
                             std::string &lastBizConfigPath) {
    auto s0 = BizConfigMaker::genSignature(deployment, templatePath, storeRoot);
    if (!s0.is_ok()) {
        AUTIL_LOG(ERROR, "gen signature failed, error: %s", s0.get_error().message().c_str());
        return false;
    }
    auto signature = s0.get();
    BizMeta meta;
    if (lastSignature != signature) {
        auto s1 = BizConfigMaker::make(deployment, templatePath, storeRoot);
        if (!s1.is_ok()) {
            AUTIL_LOG(ERROR, "make biz config failed, error msg %s", s1.get_error().message().c_str());
            return false;
        }
        meta.setRemoteConfigPath(s1.get());
        auto ret = bizMetas.emplace("default", std::move(meta));
        assert(ret.second && "biz meta should only have `default` biz");
        lastSignature = signature;
        lastBizConfigPath = s1.get();
        return true;
    }

    meta.setRemoteConfigPath(lastBizConfigPath);
    auto ret = bizMetas.emplace("default", std::move(meta));
    assert(ret.second && "biz meta should only have `default` biz");
    return true;
}

bool ZoneTarget::fillServiceInfo(const std::string &serviceInfoStr) {
    if (serviceInfoStr.empty()) {
        return true;
    }
    try {
        autil::legacy::FastFromJsonString(serviceInfo, serviceInfoStr);
    } catch (const autil::legacy::ExceptionBase &e) {
        AUTIL_LOG(ERROR, "parse service info failed");
        return false;
    }
    return true;
}

bool ZoneTarget::fillTableInfos(const std::string &rootPath,
                                const std::string &templatePath,
                                const std::vector<const Table *> &tables,
                                const std::map<catalog::PartitionId, catalog::proto::Build> &builds) {
    for (auto table : tables) {
        TableInfo tableInfo;
        for (const auto &partName : table->listPartition()) {
            Partition *part = nullptr;
            if (!isOk(table->getPartition(partName, part))) {
                AUTIL_LOG(ERROR, "get part [%s] failed", partName.c_str());
                return false;
            }
            GenerationInfo generationInfo;
            if (!generationInfo.fill(rootPath, templatePath, table, part, builds)) {
                AUTIL_LOG(ERROR, "fill generation info failed, part name [%s]", partName.c_str());
                return false;
            }
            AUTIL_LOG(INFO,
                      "table info emplace [%s], %s",
                      ToString(generationInfo.generationId).c_str(),
                      autil::legacy::FastToJsonString(generationInfo).c_str());
            tableInfo.emplace(ToString(generationInfo.generationId), generationInfo);
            shardCount = std::max(shardCount, (uint32_t)generationInfo.partitions.size());
        }
        tableInfos.emplace(table->tableName(), tableInfo);
    }
    return true;
}

autil::Result<ZoneTarget> ZoneTarget::split(int32_t roleId, int32_t roleCount) const {
    ZoneTarget target;
    target.groupName = groupName;
    target.hippoConfig = hippoConfig;
    target.shardCount = shardCount;
    target.bizMetas = bizMetas;
    for (const auto &iter : tableInfos) {
        TableInfo newTableInfo;
        for (const auto &iter2 : iter.second) {
            auto gInfo = iter2.second;
            if (!gInfo.splitPartitions(iter2.second.partitions, shardCount, roleId, roleCount)) {
                return autil::result::RuntimeError::make("split partitions failed");
            }
            newTableInfo.emplace(iter2.first, gInfo);
        }
        target.tableInfos.emplace(iter.first, newTableInfo);
    }
    return target;
}

bool ZoneTarget::operator==(const ZoneTarget &other) const {
    if (this == &other) {
        return true;
    }
    return groupName == other.groupName && tableInfos == other.tableInfos && hippoConfig == other.hippoConfig;
}

bool ZoneTarget::operator!=(const ZoneTarget &other) const { return !(*this == other); }

void AdminTarget::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) { json.Jsonize("zones", zones, zones); }

bool AdminTarget::parseFrom(const JsonNodeRef::JsonMap &jsonMap) {
    auto it = jsonMap.find("zones");
    if (it == jsonMap.end()) {
        AUTIL_LOG(INFO, "no zones in target");
        return true;
    }
    try {
        autil::legacy::FromJson(zones, it->second);
    } catch (const autil::legacy::ExceptionBase &e) {
        AUTIL_LOG(ERROR, "parse zones failed, error: %s", e.what());
        return false;
    }

    return true;
}

JsonNodeRef::JsonMap AdminTarget::toJsonMap() const {
    JsonMap jsonMap;
    jsonMap["zones"] = autil::legacy::ToJson(zones);
    return jsonMap;
}

bool AdminTarget::operator==(const AdminTarget &other) const {
    if (this == &other) {
        return true;
    }
    return zones == other.zones;
}

bool AdminTarget::operator!=(const AdminTarget &other) const { return !(*this == other); }

} // namespace suez
