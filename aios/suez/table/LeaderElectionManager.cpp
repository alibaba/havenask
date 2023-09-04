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
#include "suez/table/LeaderElectionManager.h"

#include "autil/Log.h"
#include "suez/sdk/PathDefine.h"
#include "suez/table/DefaultLeaderElectionManager.h"
#include "suez/table/LeaderElectionConfig.h"
#include "suez/table/ZkLeaderElectionManager.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, LeaderElectionManager);

LeaderElectionManager::LeaderElectionManager(const LeaderElectionConfig &config) : _config(config) {}

LeaderElectionManager::~LeaderElectionManager() {}

LeaderElectionManager *LeaderElectionManager::create(const std::string &zoneName) {
    LeaderElectionConfig config;
    if (!config.initFromEnv()) {
        return nullptr;
    }
    if (config.zkRoot == "LOCAL") {
        return new DefaultLeaderElectionManager(RT_LEADER);
    }

    AUTIL_LOG(INFO, "create ZkLeaderElectionManager with config: %s", FastToJsonString(config).c_str());
    if (!zoneName.empty() && config.needZoneName) {
        config.zkRoot = PathDefine::join(config.zkRoot, zoneName);
    }
    return new ZkLeaderElectionManager(config);
}

} // namespace suez
