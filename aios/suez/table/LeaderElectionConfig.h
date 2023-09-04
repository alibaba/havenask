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

#include "autil/legacy/jsonizable.h"
#include "suez/common/InnerDef.h"

namespace suez {

struct LeaderElectionConfig : autil::legacy::Jsonizable {
public:
    LeaderElectionConfig();

public:
    bool initFromEnv();

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    bool check() const;

public:
    std::string zkRoot;
    int64_t zkTimeoutInMs;
    int64_t leaseInMs;
    int64_t loopIntervalInMs;
    int64_t forceFollowerTimeInMs;
    std::string strategyType;
    bool campaignNow;
    bool needZoneName;
    bool extraPublishRawTable;
    CampaginLeaderType campaginLeaderType;

private:
    static constexpr int64_t DEFAULT_ZK_TIMEOUT_IN_MS = 10 * 1000;          // 10s
    static constexpr int64_t DEFAULT_LEASE_IN_MS = 60 * 1000;               // 60s
    static constexpr int64_t DEFAULT_LOOP_INTERVAL_IN_MS = 10 * 1000;       // 10s
    static constexpr int64_t DEFAULT_FORCE_FOLLOWER_TIME_IN_MS = 10 * 1000; // 10s
};

} // namespace suez
