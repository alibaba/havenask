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
#include "suez/admin/ClusterServiceState.h"

#include "autil/StringUtil.h"
#include "autil/result/Errors.h"
#include "suez/admin/InMemoryClusterServiceState.h"
#include "worker_framework/CompressedWorkerState.h"
#include "worker_framework/PathUtil.h"

using namespace worker_framework;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, ClusterServiceState);

const std::string CLUSTER_STATE_FILE = "cluster_state.pb";

ClusterServiceState::ClusterServiceState() = default;

ClusterServiceState::ClusterServiceState(std::unique_ptr<worker_framework::WorkerState> state)
    : _state(std::move(state)) {}

ClusterServiceState::~ClusterServiceState() = default;

autil::Result<ClusterServiceStateEntity> ClusterServiceState::recover() {
    std::string content;
    auto ec = _state->read(content);
    if (ec == WorkerState::EC_FAIL) {
        autil::result::RuntimeError::make("read state failed");
    }
    if (ec == WorkerState::EC_UPDATE || ec == WorkerState::EC_OK) {
        ClusterServiceStateEntity entity;
        if (!entity.ParseFromString(content)) {
            autil::result::RuntimeError::make("parse cluster service entity failed, content: [%s]", content.c_str());
        }
        return entity;
    }
    return ClusterServiceStateEntity();
}

bool ClusterServiceState::persist(
    const std::map<std::string, ClusterDeployment> &clusterMap,
    const std::map<std::pair<std::string, std::string>, DatabaseDeployment> &dbDeploymentMap) {
    ClusterServiceStateEntity entity;
    for (const auto &iter : clusterMap) {
        entity.mutable_clusterdeployments()->Add()->CopyFrom(iter.second);
    }
    for (const auto &iter : dbDeploymentMap) {
        entity.mutable_databasedeployments()->Add()->CopyFrom(iter.second);
    }
    std::string content;
    if (!entity.SerializeToString(&content)) {
        AUTIL_LOG(ERROR, "serialize to string failed");
        return false;
    }
    auto ec = _state->write(content);
    return ec != WorkerState::EC_FAIL;
}

std::unique_ptr<ClusterServiceState> ClusterServiceState::create(const std::string &uri) {
    std::string filePath;
    if (!uri.empty()) {
        filePath = PathUtil::joinPath(uri, CLUSTER_STATE_FILE);
    }
    auto state = worker_base::CompressedWorkerState::createWithFilePath(filePath, autil::CompressType::LZ4);
    if (state != nullptr) {
        return std::make_unique<ClusterServiceState>(std::move(state));
    }

    return std::make_unique<InMemoryClusterServiceState>();
}

} // namespace suez
