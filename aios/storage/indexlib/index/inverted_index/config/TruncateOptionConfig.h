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

#include <memory>

#include "autil/Log.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/index/inverted_index/config/TruncateIndexConfig.h"
#include "indexlib/index/inverted_index/config/TruncateProfile.h"
#include "indexlib/index/inverted_index/config/TruncateStrategy.h"

namespace indexlibv2::config {

typedef std::map<std::string, std::string> ProfileToStrategyNameMap;
typedef std::map<std::string, TruncateIndexConfig> TruncateIndexConfigMap;
typedef std::vector<std::shared_ptr<TruncateStrategy>> TruncateStrategyVec;
typedef std::vector<std::shared_ptr<TruncateProfile>> TruncateProfileVec;

class TruncateOptionConfig
{
public:
    // TruncateOptionConfig() = default;
    explicit TruncateOptionConfig(const std::vector<TruncateStrategy>& truncateStrategyVec);
    ~TruncateOptionConfig() = default;

public:
    void Init(const std::vector<std::shared_ptr<IIndexConfig>>& indexConfigs,
              const std::vector<TruncateProfileConfig>& truncateProfileConfigs);

    void Check(const std::vector<std::shared_ptr<IIndexConfig>>& indexConfigs,
               const std::vector<TruncateProfileConfig>& truncateProfileConfigs) const;

    bool operator==(const TruncateOptionConfig& other) const;

    bool IsTruncateIndex(const std::string& indexName) const;

    const TruncateIndexConfigMap& GetTruncateIndexConfigs() const { return _truncateIndexConfig; }

    const TruncateIndexConfig& GetTruncateIndexConfig(const std::string& indexName) const;

    const TruncateProfileVec& GetTruncateProfiles() const { return _truncateProfileVec; }

    const std::shared_ptr<TruncateProfile>& GetTruncateProfile(const std::string& profileName) const;

    const TruncateStrategyVec& GetTruncateStrategys() const { return _truncateStrategyVec; }

    const std::shared_ptr<TruncateStrategy>& GetTruncateStrategy(const std::string& strategyName) const;

    void UpdateTruncateIndexConfig(int64_t beginTime, int64_t baseTime);

private:
    void CompleteTruncateStrategy(std::shared_ptr<TruncateProfile>& profile, const TruncateStrategy& strategy);

    void CreateTruncateIndexConfigs();

    void CompleteTruncateConfig(const ProfileToStrategyNameMap& profileToStrategyMap,
                                const std::vector<std::shared_ptr<IIndexConfig>>& indexConfigs,
                                const std::vector<TruncateProfileConfig>& truncateProfileConfigs);

    void InitProfileToStrategyNameMap(ProfileToStrategyNameMap& profileToStrategyMap);

private:
    std::vector<TruncateStrategy> _oriTruncateStrategyVec;
    TruncateProfileVec _truncateProfileVec;
    TruncateStrategyVec _truncateStrategyVec;
    TruncateIndexConfigMap _truncateIndexConfig;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
