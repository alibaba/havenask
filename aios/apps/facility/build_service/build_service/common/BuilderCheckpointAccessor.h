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

#include <set>
#include <stdint.h>
#include <string>
#include <vector>

#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/util/Log.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/VersionCoord.h"
#include "indexlib/indexlib.h"

namespace build_service { namespace common {

class CheckpointAccessor;

BS_TYPEDEF_PTR(CheckpointAccessor);

class BuilderCheckpointAccessor
{
public:
    BuilderCheckpointAccessor(const CheckpointAccessorPtr& accessor);
    ~BuilderCheckpointAccessor();

private:
    BuilderCheckpointAccessor(const BuilderCheckpointAccessor&);
    BuilderCheckpointAccessor& operator=(const BuilderCheckpointAccessor&);

public:
    bool getLatestCheckpoint(const std::string& clusterName, proto::BuilderCheckpoint& checkpointInfo,
                             std::string& ckpValue);

    bool getLatestCheckpoint(const std::string& clusterName, proto::BuilderCheckpoint& checkpointInfo);

    void addCheckpoint(const std::string& clusterName, const proto::BuilderCheckpoint& checkpointInfo,
                       int32_t keepCount = 1);

    void clearCheckpoint(const std::string& clusterName);

    bool createSavepoint(const std::string& applyRole, const std::string& clusterName,
                         const proto::BuilderCheckpoint& checkpointInfo);

    bool removeSavepoint(const std::string& applyRole, const std::string& clusterName,
                         const proto::BuilderCheckpoint& checkpointInfo);

    std::set<versionid_t> getReservedVersions(const std::string& clusterName);

    ///////////////////////////for SingleBuilderTaskV2//////////////////////
    void addMasterCheckpoint(const std::string& clusterName, const proto::Range& range, versionid_t versionId,
                             const std::string& checkpoint);
    void clearMasterCheckpoint(const std::string& clusterName, const proto::Range& range);
    bool getMasterCheckpoint(const std::string& clusterName, const proto::Range& range, std::string& checkpoint);
    void addSlaveCheckpoint(const std::string& clusterName, const proto::Range& range, versionid_t versionId,
                            const std::string& checkpoint);
    void clearSlaveCheckpoint(const std::string& clusterName, const proto::Range& range);
    bool getSlaveCheckpoint(const std::string& clusterName, const proto::Range& range, std::string& checkpoint);
    std::set<indexlibv2::framework::VersionCoord> getSlaveReservedVersions(const std::string& clusterName,
                                                                           const std::vector<proto::Range>& ranges);

    /**
        bool isSavepoint(const std::string& clusterName,
                        versionid_t version);
    **/
private:
    CheckpointAccessorPtr _accessor;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuilderCheckpointAccessor);

}} // namespace build_service::common
