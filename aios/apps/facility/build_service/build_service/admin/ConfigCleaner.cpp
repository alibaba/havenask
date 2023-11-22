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
#include "build_service/admin/ConfigCleaner.h"

#include <algorithm>
#include <iosfwd>
#include <memory>
#include <set>
#include <stdint.h>
#include <string>
#include <vector>

#include "build_service/admin/CheckpointCreator.h"
#include "build_service/common/IndexCheckpointAccessor.h"
#include "build_service/config/AgentGroupConfig.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/Admin.pb.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/indexlib.h"

using namespace std;
using namespace build_service::config;

using build_service::common::IndexCheckpointAccessorPtr;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ConfigCleaner);

ConfigCleaner::ConfigCleaner(TaskResourceManagerPtr resourceManager) : _resourceManager(resourceManager) {}

ConfigCleaner::~ConfigCleaner() {}

void ConfigCleaner::cleanObsoleteConfig()
{
    IndexCheckpointAccessorPtr checkpointAccessor = CheckpointCreator::createIndexCheckpointAccessor(_resourceManager);
    ConfigReaderAccessorPtr configAccessor;
    _resourceManager->getResource(configAccessor);
    auto latestConfig = configAccessor->getLatestConfig();
    if (!checkpointAccessor || !configAccessor) {
        return;
    }
    vector<string> clusterNames;
    configAccessor->getAllClusterNames(clusterNames);
    for (auto cluster : clusterNames) {
        set<versionid_t> versionSet = checkpointAccessor->getReservedVersions(cluster);
        int64_t ckpMinSchemaId = -1;
        for (auto iter = versionSet.begin(); iter != versionSet.end(); iter++) {
            proto::CheckpointInfo checkpoint;
            checkpointAccessor->getIndexCheckpoint(cluster, *iter, checkpoint);
            if (ckpMinSchemaId == -1) {
                ckpMinSchemaId = checkpoint.schemaid();
            } else {
                ckpMinSchemaId = min(ckpMinSchemaId, checkpoint.schemaid());
            }
        }
        int64_t configMinSchemaId = configAccessor->getMinSchemaId(cluster);
        if (configMinSchemaId >= ckpMinSchemaId) {
            continue;
        }
        auto schema = latestConfig->getSchema(cluster);
        auto latestConfigSchemaId = schema->GetSchemaVersionId();
        for (auto schemaId = configMinSchemaId; schemaId < ckpMinSchemaId; schemaId++) {
            if (schemaId == latestConfigSchemaId) {
                continue;
            }
            configAccessor->deleteConfig(cluster, schemaId);
        }
    }
}

}} // namespace build_service::admin
