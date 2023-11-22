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

#include <string>
#include <vector>

#include "build_service/admin/ClusterCheckpointSynchronizer.h"
#include "build_service/common/Checkpoint.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "indexlib/base/Types.h"

namespace build_service { namespace admin {

class ZKClusterCheckpointSynchronizer : public ClusterCheckpointSynchronizer
{
public:
    ZKClusterCheckpointSynchronizer(const proto::BuildId& buildId, const std::string& clusterName,
                                    const std::string& suezVersionZkRoot);
    ~ZKClusterCheckpointSynchronizer();

    void addCheckpointToIndexInfo(indexlibv2::schemaid_t schemaVersion,
                                  const std::vector<common::PartitionLevelCheckpoint>& partitionCheckpoints,
                                  const IndexInfoParam& indexInfoParam, bool isFullIndex = false) override;
    const std::string& getSuezVersionZkRoot() const { return _suezVersionZkRoot; }

private:
    std::string _suezVersionZkRoot;

    BS_LOG_DECLARE();
};

}} // namespace build_service::admin
