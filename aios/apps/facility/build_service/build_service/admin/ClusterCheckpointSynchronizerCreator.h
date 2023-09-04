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

#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {
class ResourceReader;
}} // namespace build_service::config

namespace build_service { namespace common {
class CheckpointAccessor;
}} // namespace build_service::common

namespace build_service { namespace admin {

class ClusterCheckpointSynchronizer;
class CheckpointMetricReporter;

class ClusterCheckpointSynchronizerCreator
{
public:
    enum class Type { NONE, DEFAULT, ZOOKEEPER };
    explicit ClusterCheckpointSynchronizerCreator(const proto::BuildId& buildId);
    ~ClusterCheckpointSynchronizerCreator();

    std::map<std::string, std::shared_ptr<ClusterCheckpointSynchronizer>>
    create(const std::shared_ptr<common::CheckpointAccessor>& checkpointAccessor, const std::string& configPath,
           const std::shared_ptr<CheckpointMetricReporter>& checkpointMetricReporter, const std::string& indexRoot,
           Type type) const;

    std::map<std::string, std::shared_ptr<ClusterCheckpointSynchronizer>>
    create(const std::shared_ptr<common::CheckpointAccessor>& checkpointAccessor,
           const std::shared_ptr<config::ResourceReader>& configReader,
           const std::shared_ptr<CheckpointMetricReporter>& checkpointMetricReporter, const std::string& indexRoot,
           Type type) const;

private:
    std::shared_ptr<ClusterCheckpointSynchronizer>
    createSingle(const std::string& clusterName, const std::shared_ptr<common::CheckpointAccessor>& checkpointAccessor,
                 const std::shared_ptr<config::ResourceReader>& configReader,
                 const std::shared_ptr<CheckpointMetricReporter>& checkpointMetricReporter,
                 const std::string& indexRoot, const std::string& suezVersionZkRoot, Type type) const;

    bool getPartitionCount(const std::shared_ptr<config::ResourceReader>& configReader, const std::string& clusterName,
                           uint32_t* partitionCount) const;
    bool getSuezVersionZkRoots(const std::shared_ptr<config::ResourceReader>& configReader,
                               std::map<std::string, std::string>* suezVersionZkRoots) const;

private:
    proto::BuildId _buildId;

    BS_LOG_DECLARE();
};

}} // namespace build_service::admin
