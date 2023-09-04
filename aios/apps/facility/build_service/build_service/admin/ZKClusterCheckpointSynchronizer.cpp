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
#include "build_service/admin/ZKClusterCheckpointSynchronizer.h"

#include "autil/EnvUtil.h"

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ZKClusterCheckpointSynchronizer);

ZKClusterCheckpointSynchronizer::ZKClusterCheckpointSynchronizer(const proto::BuildId& buildId,
                                                                 const std::string& clusterName,
                                                                 const std::string& suezVersionZkRoot)
    : ClusterCheckpointSynchronizer(buildId, clusterName)
    , _suezVersionZkRoot(suezVersionZkRoot)
{
}

ZKClusterCheckpointSynchronizer::~ZKClusterCheckpointSynchronizer() {}

void ZKClusterCheckpointSynchronizer::addCheckpointToIndexInfo(
    indexlibv2::schemaid_t schemaVersion, const std::vector<common::PartitionLevelCheckpoint>& partitionCheckpoints,
    const IndexInfoParam& indexInfoParam, bool isFullIndex)
{
    static bool needAdd = autil::EnvUtil::getEnv("PUBLISH_VERSION_FROM_ZK", false);
    if (needAdd) {
        ClusterCheckpointSynchronizer::addCheckpointToIndexInfo(schemaVersion, partitionCheckpoints, indexInfoParam);
    }
}

}} // namespace build_service::admin
