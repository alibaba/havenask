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

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {

class AgentConfig : public autil::legacy::Jsonizable
{
public:
    struct ResourceLimitParam : public autil::legacy::Jsonizable {
    public:
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("resource", resourceName);
            json.Jsonize("oversell_factor", oversellFactor, oversellFactor);
        }

    public:
        std::string resourceName;
        double oversellFactor = 1.0;
    };

public:
    AgentConfig();
    ~AgentConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool check() const;

public:
    static constexpr int64_t DEFAULT_IN_BLACKLIST_TIMEOUT = 60 * 20; // 20 min
    std::string identifier;
    std::string rolePattern;
    uint32_t agentNodeCount;
    int64_t inBlackListTimeout;
    bool lazyAllocate;
    bool dynamicRoleMapping;
    std::vector<ResourceLimitParam> resourceLimitParams;

private:
    BS_LOG_DECLARE();
};

class AgentGroupConfig : public autil::legacy::Jsonizable
{
public:
    AgentGroupConfig();
    ~AgentGroupConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool check() const;
    bool match(const std::string& roleName, size_t& idx) const;
    const std::vector<AgentConfig>& getAgentConfigs() const { return _agentConfigs; }
    size_t size() const { return _agentConfigs.size(); }
    const AgentConfig& operator[](size_t idx) const { return _agentConfigs[idx]; }
    bool needDynamicMapping() const;

private:
    std::vector<AgentConfig> _agentConfigs;
    mutable std::map<std::string, int32_t> _roleMatchInfoCache;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(AgentGroupConfig);

}} // namespace build_service::config
