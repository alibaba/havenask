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
#include "suez/table/LeaderElectionConfig.h"

#include "autil/EnvUtil.h"
#include "autil/Log.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, LeaderElectionConfig);

LeaderElectionConfig::LeaderElectionConfig()
    : zkTimeoutInMs(DEFAULT_ZK_TIMEOUT_IN_MS)
    , leaseInMs(DEFAULT_LEASE_IN_MS)
    , loopIntervalInMs(DEFAULT_LOOP_INTERVAL_IN_MS)
    , forceFollowerTimeInMs(DEFAULT_FORCE_FOLLOWER_TIME_IN_MS)
    , campaignNow(true)
    , needZoneName(true)
    , extraPublishRawTable(false)
    , campaginLeaderType(CLT_NORMAL) {}

bool LeaderElectionConfig::initFromEnv() {
    auto configStr = autil::EnvUtil::getEnv("leader_election_config", "");
    if (!configStr.empty()) {
        try {
            autil::legacy::FromJsonString(*this, configStr);
            return check();
        } catch (const std::exception &e) {
            AUTIL_LOG(ERROR, "parse leader_election_config: %s failed, error: %s", configStr.c_str(), e.what());
            return false;
        }
    } else {
        auto zkRoot = autil::EnvUtil::getEnv("zk_root", "");
        if (zkRoot.empty()) {
            AUTIL_LOG(ERROR, "both leader_election_config and zk_root are not specified");
            return false;
        }
        this->zkRoot = zkRoot;
        this->strategyType = autil::EnvUtil::getEnv("leader_election_strategy_type", "");
        // other parameters are default
        return true;
    }
}

void LeaderElectionConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("zk_root", zkRoot, zkRoot);
    json.Jsonize("zk_timeout_in_ms", zkTimeoutInMs, zkTimeoutInMs);
    json.Jsonize("lease_in_ms", leaseInMs, leaseInMs);
    json.Jsonize("loop_interval_in_ms", loopIntervalInMs, loopIntervalInMs);
    json.Jsonize("strategy_type", strategyType, strategyType);
    json.Jsonize("force_follower_time_in_ms", forceFollowerTimeInMs, forceFollowerTimeInMs);
    json.Jsonize("campaign_now", campaignNow, campaignNow);
    json.Jsonize("need_zone_name", needZoneName, needZoneName);
    json.Jsonize("extra_publish_raw_table", extraPublishRawTable, extraPublishRawTable);
    std::string typeStr = "normal";
    if (FROM_JSON == json.GetMode()) {
        json.Jsonize("campagin_leader_type", typeStr, typeStr);
        if (typeStr == "normal") {
            campaginLeaderType = CLT_NORMAL;
        } else if (typeStr == "by_indication") {
            campaginLeaderType = CLT_BY_INDICATION;
        } else {
            campaginLeaderType = CLT_UNKNOWN;
        }
    } else {
        if (campaginLeaderType == CLT_NORMAL) {
            typeStr = "normal";
        } else if (campaginLeaderType == CLT_BY_INDICATION) {
            typeStr = "by_indication";
        } else {
            typeStr = "unknown";
        }
        json.Jsonize("campagin_leader_type", typeStr, typeStr);
    }
}

bool LeaderElectionConfig::check() const { return !zkRoot.empty() && campaginLeaderType != CLT_UNKNOWN; }

} // namespace suez
