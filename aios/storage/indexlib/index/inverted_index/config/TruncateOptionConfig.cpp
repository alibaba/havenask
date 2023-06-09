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
#include "indexlib/index/inverted_index/config/TruncateOptionConfig.h"

#include "autil/legacy/exception.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, TruncateOptionConfig);

TruncateOptionConfig::TruncateOptionConfig(const std::vector<TruncateStrategy>& truncateStrategyVec)
    : _oriTruncateStrategyVec(truncateStrategyVec)
{
}

void TruncateOptionConfig::Init(const std::vector<std::shared_ptr<IIndexConfig>>& indexConfigs,
                                const std::vector<TruncateProfileConfig>& truncateProfileConfigs)
{
    if (_truncateIndexConfig.size() > 0 || _oriTruncateStrategyVec.empty()) {
        // already completed
        return;
    }

    if (!indexConfigs.empty() && !truncateProfileConfigs.empty()) {
        AUTIL_LOG(INFO, "Combine truncate config with schema");
        ProfileToStrategyNameMap profileToStrategyMap;
        InitProfileToStrategyNameMap(profileToStrategyMap);
        CompleteTruncateConfig(profileToStrategyMap, indexConfigs, truncateProfileConfigs);
    }
    CreateTruncateIndexConfigs();
    Check(indexConfigs, truncateProfileConfigs);
}

const std::shared_ptr<TruncateProfile>& TruncateOptionConfig::GetTruncateProfile(const std::string& profileName) const
{
    for (size_t i = 0; i < _truncateProfileVec.size(); ++i) {
        if (_truncateProfileVec[i]->truncateProfileName == profileName) {
            return _truncateProfileVec[i];
        }
    }
    const static std::shared_ptr<TruncateProfile> emptyInstance = nullptr;
    return emptyInstance;
}

const std::shared_ptr<TruncateStrategy>&
TruncateOptionConfig::GetTruncateStrategy(const std::string& strategyName) const
{
    for (size_t i = 0; i < _truncateStrategyVec.size(); ++i) {
        if (_truncateStrategyVec[i]->GetStrategyName() == strategyName) {
            return _truncateStrategyVec[i];
        }
    }
    const static std::shared_ptr<TruncateStrategy> emptyInstance = nullptr;
    return emptyInstance;
}

void TruncateOptionConfig::InitProfileToStrategyNameMap(ProfileToStrategyNameMap& profileToStrategyMap)
{
    profileToStrategyMap.clear();
    std::unordered_set<std::string> truncateStrategyFilter;
    for (size_t i = 0; i < _oriTruncateStrategyVec.size(); ++i) {
        const std::vector<std::string>& profiles = _oriTruncateStrategyVec[i].GetProfileNames();
        const std::string& strategyName = _oriTruncateStrategyVec[i].GetStrategyName();
        auto iter = truncateStrategyFilter.find(strategyName);
        if (iter != truncateStrategyFilter.end()) {
            AUTIL_LOG(ERROR, "strategy name [%s] is duplicate", strategyName.c_str());
            AUTIL_LEGACY_THROW(autil::legacy::ParameterInvalidException, "duplicate truncate strategy");
        }
        truncateStrategyFilter.insert(iter, strategyName);
        for (size_t j = 0; j < profiles.size(); ++j) {
            const std::string& profileName = profiles[j];
            if (profileToStrategyMap.find(profileName) != profileToStrategyMap.end()) {
                AUTIL_LOG(ERROR, "profile name [%s] is duplicate in truncate_strategy[%s, %s]", profileName.c_str(),
                          profileToStrategyMap[profileName].c_str(), strategyName.c_str());
                AUTIL_LEGACY_THROW(autil::legacy::ParameterInvalidException, "duplicate truncate profile");
            }
            profileToStrategyMap[profileName] = strategyName;
        }
    }
}

void TruncateOptionConfig::CompleteTruncateStrategy(std::shared_ptr<TruncateProfile>& profile,
                                                    const TruncateStrategy& truncateStrategy)
{
    auto strategy = std::make_shared<TruncateStrategy>(truncateStrategy);
    if (strategy->GetStrategyType() == "truncate_meta" && strategy->GetDiversityConstrain().GetFilterField().empty() &&
        profile->sortParams.size() >= 1) {
        // generate new stategy completed with FilterField by SortParams[0]
        const std::string& firstSortField = profile->sortParams[0].GetSortField();
        strategy->GetDiversityConstrain().SetFilterField(firstSortField);
        strategy->SetStrategyName(firstSortField + "_" + strategy->GetStrategyName());
        profile->truncateStrategyName = strategy->GetStrategyName();
    }
    strategy->GetDiversityConstrain().Init();
    _truncateStrategyVec.emplace_back(std::move(strategy));
}

void TruncateOptionConfig::CompleteTruncateConfig(
    const ProfileToStrategyNameMap& profileToStrategyMap,
    const std::vector<std::shared_ptr<config::IIndexConfig>>& indexConfigs,
    const std::vector<TruncateProfileConfig>& truncateProfileConfigs)
{
    for (const auto& truncateProfileConfig : truncateProfileConfigs) {
        auto profile = std::make_shared<TruncateProfile>(truncateProfileConfig);
        // complete IndexNames
        for (const auto& indexConfig : indexConfigs) {
            auto invertedIndexConfig = std::dynamic_pointer_cast<InvertedIndexConfig>(indexConfig);
            assert(invertedIndexConfig != nullptr);
            if (invertedIndexConfig->GetShardingType() == config::InvertedIndexConfig::IST_NEED_SHARDING) {
                for (const auto& shardConfig : invertedIndexConfig->GetShardingIndexConfigs()) {
                    auto shardInvertedIndexConfig = std::dynamic_pointer_cast<InvertedIndexConfig>(shardConfig);
                    assert(shardInvertedIndexConfig != nullptr);
                    if (shardInvertedIndexConfig->HasTruncate() &&
                        shardInvertedIndexConfig->HasTruncateProfile(&truncateProfileConfig)) {
                        profile->indexNames.push_back(shardInvertedIndexConfig->GetIndexName());
                    }
                }
            } else {
                if (invertedIndexConfig->HasTruncate() &&
                    invertedIndexConfig->HasTruncateProfile(&truncateProfileConfig)) {
                    profile->indexNames.push_back(invertedIndexConfig->GetIndexName());
                }
            }
        }
        // complete with ProfileToStrategyNameMap
        ProfileToStrategyNameMap::const_iterator it = profileToStrategyMap.find(profile->truncateProfileName);
        if (it != profileToStrategyMap.end()) {
            profile->truncateStrategyName = it->second;
        }

        // complete TruncateStrategy
        for (size_t i = 0; i < _oriTruncateStrategyVec.size(); ++i) {
            if (profile->truncateStrategyName == _oriTruncateStrategyVec[i].GetStrategyName()) {
                CompleteTruncateStrategy(profile, _oriTruncateStrategyVec[i]);
            }
        }
        AUTIL_LOG(INFO, "Generated a truncate profile[%s] with strategy[%s], has %lu indexes",
                  profile->truncateProfileName.c_str(), profile->truncateStrategyName.c_str(),
                  profile->indexNames.size());
        _truncateProfileVec.emplace_back(std::move(profile));
    }
}

void TruncateOptionConfig::Check(const std::vector<std::shared_ptr<IIndexConfig>>& indexConfigs,
                                 const std::vector<TruncateProfileConfig>& truncateProfileConfigs) const
{
    for (size_t i = 0; i < _truncateProfileVec.size(); ++i) {
        if (_truncateProfileVec[i]->Check()) {
            const std::string& strategyName = _truncateProfileVec[i]->truncateStrategyName;
            bool valid = false;
            for (size_t j = 0; j < _truncateStrategyVec.size(); ++j) {
                if (_truncateStrategyVec[j]->GetStrategyName() == strategyName) {
                    valid = true;
                    break;
                }
            }
            if (!valid) {
                AUTIL_LEGACY_THROW(autil::legacy::ParameterInvalidException, "no specified truncate strategy");
            }
        } else {
            AUTIL_LEGACY_THROW(autil::legacy::ParameterInvalidException, "invalid truncate profile");
        }
    }
    for (size_t i = 0; i < _truncateStrategyVec.size(); ++i) {
        if (!_truncateStrategyVec[i]->Check()) {
            AUTIL_LEGACY_THROW(autil::legacy::ParameterInvalidException, "invalid truncate profile");
        }
    }

    // Check that all truncate profiles under the same index use the same payload name.
    std::map<std::string, TruncateProfileConfig> name2ProfileConfig;
    for (auto& config : truncateProfileConfigs) {
        name2ProfileConfig[config.GetTruncateProfileName()] = config;
    }
    for (auto& indexConfig : indexConfigs) {
        auto invertedIndexConfig = std::dynamic_pointer_cast<InvertedIndexConfig>(indexConfig);
        assert(invertedIndexConfig != nullptr);
        const std::vector<std::string> profileNames = invertedIndexConfig->GetUseTruncateProfiles();
        if (profileNames.empty()) {
            continue;
        }
        std::string payloadName;
        for (const std::string& profileName : profileNames) {
            auto it = name2ProfileConfig.find(profileName);
            if (it == name2ProfileConfig.end()) {
                AUTIL_LEGACY_THROW(autil::legacy::ParameterInvalidException, "no truncate profile for name");
            }
            const auto& truncateProfileConfig = it->second;
            if (truncateProfileConfig.GetPayloadConfig().IsInitialized()) {
                if (!payloadName.empty()) {
                    if (payloadName != truncateProfileConfig.GetPayloadName()) {
                        AUTIL_LEGACY_THROW(autil::legacy::ParameterInvalidException,
                                           "all truncate profiles under the same index must use the same payload name");
                    }
                } else {
                    payloadName = truncateProfileConfig.GetPayloadName();
                }
            }
        }
    }
}

bool TruncateOptionConfig::operator==(const TruncateOptionConfig& other) const
{
    if (_oriTruncateStrategyVec.size() != other._oriTruncateStrategyVec.size()) {
        return false;
    }

    for (size_t i = 0; i < _oriTruncateStrategyVec.size(); ++i) {
        if (!(_oriTruncateStrategyVec[i] == other._oriTruncateStrategyVec[i])) {
            return false;
        }
    }
    return true;
}

bool TruncateOptionConfig::IsTruncateIndex(const std::string& indexName) const
{
    return (_truncateIndexConfig.find(indexName) != _truncateIndexConfig.end());
}

void TruncateOptionConfig::CreateTruncateIndexConfigs()
{
    for (size_t i = 0; i < _truncateProfileVec.size(); ++i) {
        const std::shared_ptr<TruncateProfile>& truncateProfile = _truncateProfileVec[i];
        TruncateIndexProperty property;

        property.truncateProfile = truncateProfile;
        property.truncateStrategy = GetTruncateStrategy(truncateProfile->truncateStrategyName);
        if (!property.truncateStrategy) {
            AUTIL_LOG(ERROR, "truncate strategy[%s] not exist.", truncateProfile->truncateStrategyName.c_str());
            AUTIL_LEGACY_THROW(autil::legacy::ParameterInvalidException,
                               "invalid strategy name:" + truncateProfile->truncateStrategyName +
                                   ", unable to find corresponding truncate_strategy in cluster config.");
        }

        for (size_t j = 0; j < truncateProfile->indexNames.size(); ++j) {
            const std::string& indexName = truncateProfile->indexNames[j];
            property.truncateIndexName =
                InvertedIndexConfig::CreateTruncateIndexName(indexName, truncateProfile->truncateProfileName);
            TruncateIndexConfigMap::iterator iter = _truncateIndexConfig.find(indexName);
            if (iter != _truncateIndexConfig.end()) {
                iter->second.AddTruncateIndexProperty(property);
            } else {
                _truncateIndexConfig[indexName].SetIndexName(indexName);
                _truncateIndexConfig[indexName].AddTruncateIndexProperty(property);
            }
        }
    }
}

const TruncateIndexConfig& TruncateOptionConfig::GetTruncateIndexConfig(const std::string& indexName) const
{
    TruncateIndexConfigMap::const_iterator iter = _truncateIndexConfig.find(indexName);
    if (_truncateIndexConfig.end() == iter) {
        std::string error = "fail to get truncate index config, invalid index name:" + indexName;
        AUTIL_LEGACY_THROW(autil::legacy::ParameterInvalidException, error.c_str());
    }
    return iter->second;
}

void TruncateOptionConfig::UpdateTruncateIndexConfig(int64_t beginTime, int64_t baseTime)
{
    for (size_t i = 0; i < _truncateStrategyVec.size(); ++i) {
        _truncateStrategyVec[i]->GetDiversityConstrain().ParseTimeStampFilter(beginTime, baseTime);
    }
}

} // namespace indexlibv2::config
