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
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/legacy/legacy_jsonizable_dec.h"
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
    uint32_t flexibleIdleAgentCount; // 大于0则会触发尽量保持idle agent节点个数不变的动态调度策略，
                                     // rolePlan频繁变化有可能触发agent节点频繁的分配和释放
                                     // 可能存在分配任务同时创建agent节点的极端case
    int64_t inBlackListTimeout;
    bool lazyAllocate;
    bool dynamicRoleMapping;
    bool exclusive;
    std::vector<ResourceLimitParam> resourceLimitParams;

private:
    BS_LOG_DECLARE();
};

class AgentGroupConfig : public autil::legacy::Jsonizable
{
public:
    AgentGroupConfig(bool defaultEnableGlobalAgent = false);
    ~AgentGroupConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool check() const;
    bool match(const std::string& roleName, size_t& idx) const;
    const std::vector<AgentConfig>& getAgentConfigs() const { return _agentConfigs; }
    size_t size() const { return _agentConfigs.size(); }
    const AgentConfig& operator[](size_t idx) const { return _agentConfigs[idx]; }
    bool needDynamicMapping() const;
    bool enableGlobalAgentNodes() const { return _enableGlobalAgentNodes; }

    const std::string& getTargetGlobalAgentGroupId() const { return _targetGlobalAgentGroupName; }

public:
    static const std::string AGENT_CONFIG_GROUP_KEY;

private:
    static constexpr int64_t DEFAULT_CACHE_EXPIRE_TIME = 1800; // 30 min

    std::vector<AgentConfig> _agentConfigs;
    bool _enableGlobalAgentNodes = false;
    std::string _targetGlobalAgentGroupName; /* for generation level agent */
    mutable std::map<std::string, int32_t> _roleMatchInfoCache;
    mutable int64_t _cacheExpireTimestamp = -1;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(AgentGroupConfig);

}} // namespace build_service::config
