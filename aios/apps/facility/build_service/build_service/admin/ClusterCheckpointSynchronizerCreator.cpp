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
#include "build_service/admin/ClusterCheckpointSynchronizerCreator.h"

#include "build_service//admin/ClusterCheckpointSynchronizer.h"
#include "build_service//admin/ZKClusterCheckpointSynchronizer.h"
#include "build_service/admin/CheckpointSynchronizer.h"
#include "build_service/admin/ZKCheckpointSynchronizer.h"
#include "build_service/config/BuildRuleConfig.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/util/RangeUtil.h"

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ClusterCheckpointSynchronizerCreator);

#define CS_LOG(level, format, args...) BS_LOG(level, "[%s] " format, _buildId.ShortDebugString().c_str(), ##args)
#define CS_INTERVAL_LOG(interval, level, format, args...)                                                              \
    BS_INTERVAL_LOG(interval, level, "[%s] " format, _buildId.ShortDebugString().c_str(), ##args)

ClusterCheckpointSynchronizerCreator::ClusterCheckpointSynchronizerCreator(const proto::BuildId& buildId)
    : _buildId(buildId)
{
}

ClusterCheckpointSynchronizerCreator::~ClusterCheckpointSynchronizerCreator() {}

std::map<std::string, std::shared_ptr<ClusterCheckpointSynchronizer>>
ClusterCheckpointSynchronizerCreator::create(const std::shared_ptr<common::CheckpointAccessor>& checkpointAccessor,
                                             const std::string& configPath,
                                             const std::shared_ptr<CheckpointMetricReporter>& checkpointMetricReporter,
                                             const std::string& indexRoot, Type type) const
{
    auto configReader = config::ResourceReaderManager::getResourceReader(configPath);
    return create(checkpointAccessor, configReader, checkpointMetricReporter, indexRoot, type);
}

std::shared_ptr<ClusterCheckpointSynchronizer> ClusterCheckpointSynchronizerCreator::createSingle(
    const std::string& clusterName, const std::shared_ptr<common::CheckpointAccessor>& checkpointAccessor,
    const std::shared_ptr<config::ResourceReader>& configReader,
    const std::shared_ptr<CheckpointMetricReporter>& checkpointMetricReporter, const std::string& indexRoot,
    const std::string& suezVersionZkRoot, Type type) const
{
    uint32_t partitionCount = 0;
    if (!getPartitionCount(configReader, clusterName, &partitionCount)) {
        CS_LOG(ERROR, "get partition count failed, clusterName[%s]", clusterName.c_str());
        return nullptr;
    }
    if (partitionCount == 0) {
        CS_LOG(ERROR, "invalid partition count [%u], clusterName[%s]", partitionCount, clusterName.c_str());
        return nullptr;
    }
    auto rangeVec = util::RangeUtil::splitRange(0, 65535, partitionCount);
    auto tabletOptions = configReader->getTabletOptions(clusterName);
    ClusterCheckpointSynchronizer::Options options;
    if (tabletOptions) {
        if (auto status = tabletOptions->GetFromRawJson("offline_index_config.build_config.checkpoint_sync_interval_ms",
                                                        &options.syncIntervalMs);
            !status.IsOKOrNotFound()) {
            CS_LOG(ERROR, "get config from tablet options failed, status[%s]", status.ToString().c_str());
        }
        if (auto status = tabletOptions->GetFromRawJson("offline_index_config.build_config.keep_daily_checkpoint_count",
                                                        &options.reserveDailyCheckpointCount);
            !status.IsOKOrNotFound()) {
            CS_LOG(ERROR, "get config from tablet options failed, status[%s]", status.ToString().c_str());
        }
        if (auto status = tabletOptions->GetFromRawJson("offline_index_config.build_config.keep_version_count",
                                                        &options.reserveCheckpointCount);
            !status.IsOKOrNotFound()) {
            CS_LOG(ERROR, "get config from tablet options failed, status[%s]", status.ToString().c_str());
        }
    }
    std::shared_ptr<ClusterCheckpointSynchronizer> clusterSynchronizer;
    if (type == Type::DEFAULT) {
        clusterSynchronizer = std::make_shared<ClusterCheckpointSynchronizer>(_buildId, clusterName);
    } else if (type == Type::ZOOKEEPER) {
        clusterSynchronizer =
            std::make_shared<ZKClusterCheckpointSynchronizer>(_buildId, clusterName, suezVersionZkRoot);
    } else {
        CS_LOG(ERROR, "create checkpoint synchronizer failed, invalid type[%d], clusterName[%s].", (int)type,
               clusterName.c_str());
        return nullptr;
    }
    if (clusterSynchronizer->init(indexRoot, rangeVec, checkpointAccessor, checkpointMetricReporter, options)) {
        return clusterSynchronizer;
    } else {
        CS_LOG(ERROR, "cluster synchronizer init failed, clusterName[%s]", clusterName.c_str());
    }
    return nullptr;
}

std::map<std::string, std::shared_ptr<ClusterCheckpointSynchronizer>>
ClusterCheckpointSynchronizerCreator::create(const std::shared_ptr<common::CheckpointAccessor>& checkpointAccessor,
                                             const std::shared_ptr<config::ResourceReader>& configReader,
                                             const std::shared_ptr<CheckpointMetricReporter>& checkpointMetricReporter,
                                             const std::string& indexRoot, Type type) const
{
    if (checkpointAccessor == nullptr) {
        CS_LOG(ERROR, "checkpoint accessor is nullptr");
        return {};
    }
    if (configReader == nullptr) {
        CS_LOG(ERROR, "resource reader is nullptr");
        return {};
    }
    std::vector<std::string> clusterNames;
    if (!configReader->getAllClusters(_buildId.datatable(), clusterNames) || clusterNames.empty()) {
        CS_LOG(ERROR, "getAllClusters for [%s] failed", _buildId.datatable().c_str());
        return {};
    }
    std::map<std::string, std::string> suezVersionZkRoots;
    if (type == Type::ZOOKEEPER && !getSuezVersionZkRoots(configReader, &suezVersionZkRoots)) {
        CS_LOG(ERROR, "type is ZK but no suez version zk root is found");
        return {};
    }
    std::map<std::string, std::shared_ptr<ClusterCheckpointSynchronizer>> clusterSynchronizers;
    for (const auto& clusterName : clusterNames) {
        std::string suezVersionZkRoot;
        if (type == Type::ZOOKEEPER) {
            auto iter = suezVersionZkRoots.find(clusterName);
            if (iter == suezVersionZkRoots.end()) {
                continue;
            }
            suezVersionZkRoot = iter->second;
        }
        auto singleClusterSynchronizer = createSingle(clusterName, checkpointAccessor, configReader,
                                                      checkpointMetricReporter, indexRoot, suezVersionZkRoot, type);
        if (singleClusterSynchronizer == nullptr) {
            return {};
        }
        clusterSynchronizers[clusterName] = singleClusterSynchronizer;
    }
    return clusterSynchronizers;
}

bool ClusterCheckpointSynchronizerCreator::getPartitionCount(
    const std::shared_ptr<config::ResourceReader>& configReader, const std::string& clusterName,
    uint32_t* partitionCount) const
{
    config::BuildRuleConfig buildRuleConfig;
    if (!configReader->getClusterConfigWithJsonPath(clusterName, "cluster_config.builder_rule_config",
                                                    buildRuleConfig)) {
        CS_LOG(ERROR, "get builder_rule_config failed, cluster[%s]", clusterName.c_str());
        return false;
    }
    *partitionCount = buildRuleConfig.partitionCount;
    return true;
}

bool ClusterCheckpointSynchronizerCreator::getSuezVersionZkRoots(
    const std::shared_ptr<config::ResourceReader>& configReader,
    std::map<std::string, std::string>* suezVersionZkRoots) const
{
    config::BuildServiceConfig buildServiceConfig;
    if (!configReader->getConfigWithJsonPath("build_app.json", "", buildServiceConfig)) {
        CS_LOG(ERROR, "failed to get build_app.json");
        return false;
    }
    *suezVersionZkRoots = buildServiceConfig.suezVersionZkRoots;
    return true;
}
}} // namespace build_service::admin
