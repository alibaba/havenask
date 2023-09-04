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

#include <map>
#include <memory>

#include "autil/result/Result.h"
#include "suez/admin/ClusterService.pb.h"

namespace worker_framework {
class WorkerState;
}

namespace suez {

extern const std::string ZK_PREFIX;
extern const std::string LOCAL_PREFIX;
extern const std::string CLUSTER_STATE_FILE;

class ClusterServiceState {
public:
    ClusterServiceState();
    ClusterServiceState(std::unique_ptr<worker_framework::WorkerState> state);
    virtual ~ClusterServiceState();

public:
    virtual autil::Result<ClusterServiceStateEntity> recover();
    virtual bool persist(const std::map<std::string, ClusterDeployment> &clusterDeploymentMap,
                         const std::map<std::pair<std::string, std::string>, DatabaseDeployment> &dbDeploymentMap);

public:
    static std::unique_ptr<ClusterServiceState> create(const std::string &uri);

private:
    std::unique_ptr<worker_framework::WorkerState> _state;
};

} // namespace suez
