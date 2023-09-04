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

#include "build_service/admin/CheckpointSynchronizer.h"
#include "build_service/admin/ClusterCheckpointSynchronizer.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/util/Log.h"

namespace build_service { namespace common {

class IndexCheckpointAccessor;
BS_TYPEDEF_PTR(IndexCheckpointAccessor);
}} // namespace build_service::common

namespace build_service { namespace admin {

class LegacyCheckpointAdaptor : public ClusterCheckpointSynchronizer
{
public:
    LegacyCheckpointAdaptor(const std::shared_ptr<config::ResourceReader>& resourceReader,
                            const common::IndexCheckpointAccessorPtr& accessor, const proto::BuildId& buildId,
                            const std::string& clusterName, const std::string& indexRoot);
    ~LegacyCheckpointAdaptor();

public:
    bool
    listCheckpoint(bool savepointOnly, uint32_t offset, uint32_t limit,
                   std::vector<admin::ClusterCheckpointSynchronizer::SingleGenerationLevelCheckpointResult>* result);
    bool getCheckpoint(checkpointid_t checkpointId,
                       ClusterCheckpointSynchronizer::GenerationLevelCheckpointGetResult* result);
    bool createSavepoint(checkpointid_t checkpointId,
                         ClusterCheckpointSynchronizer::GenerationLevelCheckpointGetResult* result);
    bool removeSavepoint(checkpointid_t checkpointId);

private:
    bool getPartitionCount(uint32_t* partitionCount);

private:
    const std::shared_ptr<config::ResourceReader> _resourceReader;
    const common::IndexCheckpointAccessorPtr _accessor;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(LegacyCheckpointAdaptor);

}} // namespace build_service::admin
