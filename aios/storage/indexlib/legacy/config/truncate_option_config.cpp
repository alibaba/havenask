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
#include "indexlib/config/truncate_option_config.h"

#include "indexlib/config/configurator_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/util/Exception.h"
using namespace std;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, TruncateOptionConfig);

TruncateOptionConfig::TruncateOptionConfig(const vector<TruncateStrategy>& truncateStrategyVec)
    : mOriTruncateStrategyVec(truncateStrategyVec)
{
}

TruncateOptionConfig::~TruncateOptionConfig() {}

const TruncateProfilePtr& TruncateOptionConfig::GetTruncateProfile(const std::string& profileName) const
{
    for (size_t i = 0; i < mTruncateProfileVec.size(); ++i) {
        if (mTruncateProfileVec[i]->mTruncateProfileName == profileName) {
            return mTruncateProfileVec[i];
        }
    }
    return mDefaultTruncateProfilePtr;
}

const TruncateStrategyPtr& TruncateOptionConfig::GetTruncateStrategy(const std::string& strategyName) const
{
    for (size_t i = 0; i < mTruncateStrategyVec.size(); ++i) {
        if (mTruncateStrategyVec[i]->GetStrategyName() == strategyName) {
            return mTruncateStrategyVec[i];
        }
    }
    return mDefaultTruncateStrategyPtr;
}

void TruncateOptionConfig::InitProfileToStrategyNameMap(ProfileToStrategyNameMap& profileToStrategyMap)
{
    profileToStrategyMap.clear();
    for (size_t i = 0; i < mOriTruncateStrategyVec.size(); ++i) {
        const vector<string>& profiles = mOriTruncateStrategyVec[i].GetProfileNames();
        const string& strategyName = mOriTruncateStrategyVec[i].GetStrategyName();
        for (size_t j = 0; j < profiles.size(); ++j) {
            const string& profileName = profiles[j];
            if (profileToStrategyMap.find(profileName) != profileToStrategyMap.end()) {
                INDEXLIB_FATAL_ERROR(BadParameter, "profile name [%s] is duplicate in truncate_strategy[%s, %s]",
                                     profileName.c_str(), profileToStrategyMap[profileName].c_str(),
                                     strategyName.c_str());
            }
            profileToStrategyMap[profileName] = strategyName;
        }
    }
}

void TruncateOptionConfig::CompleteTruncateStrategy(TruncateProfilePtr& profilePtr, const TruncateStrategy& strategy)
{
    TruncateStrategyPtr strategyPtr(new TruncateStrategy(strategy));
    if (TRUNCATE_META_STRATEGY_TYPE == strategyPtr->GetStrategyType() &&
        strategyPtr->GetDiversityConstrain().GetFilterField().empty() && profilePtr->mSortParams.size() >= 1) {
        // generate new stategy completed with FilterField by SortParams[0]
        const string& firstSortField = profilePtr->mSortParams[0].GetSortField();
        strategyPtr->GetDiversityConstrain().SetFilterField(firstSortField);
        strategyPtr->SetStrategyName(firstSortField + "_" + strategyPtr->GetStrategyName());
        profilePtr->mTruncateStrategyName = strategyPtr->GetStrategyName();
    }
    strategyPtr->GetDiversityConstrain().Init();

    // skip duplicate strategy
    for (size_t i = 0; i < mTruncateStrategyVec.size(); ++i) {
        if (mTruncateStrategyVec[i]->GetStrategyName() == strategyPtr->GetStrategyName()) {
            return;
        }
    }
    mTruncateStrategyVec.push_back(strategyPtr);
}

void TruncateOptionConfig::CompleteTruncateConfig(ProfileToStrategyNameMap& profileToStrategyMap,
                                                  const IndexPartitionSchemaPtr& schema)
{
    const TruncateProfileSchemaPtr& truncateProfileSchema = schema->GetTruncateProfileSchema();
    for (TruncateProfileSchema::Iterator iter = truncateProfileSchema->Begin(); iter != truncateProfileSchema->End();
         ++iter) {
        TruncateProfilePtr profilePtr(new TruncateProfile(*(iter->second)));

        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
        // complete IndexNames
        auto indexConfigs = indexSchema->CreateIterator(false);
        for (auto indexIt = indexConfigs->Begin(); indexIt != indexConfigs->End(); indexIt++) {
            const TruncateProfileConfigPtr& truncateProfileConfig =
                truncateProfileSchema->GetTruncateProfileConfig(profilePtr->mTruncateProfileName);
            if ((*indexIt)->HasTruncate() && truncateProfileConfig != nullptr &&
                (*indexIt)->HasTruncateProfile(truncateProfileConfig.get())) {
                profilePtr->mIndexNames.push_back((*indexIt)->GetIndexName());
            }
        }
        // complete with ProfileToStrategyNameMap
        ProfileToStrategyNameMap::const_iterator it = profileToStrategyMap.find(profilePtr->mTruncateProfileName);
        if (it != profileToStrategyMap.end()) {
            profilePtr->mTruncateStrategyName = it->second;
        }
        // complete TruncateStrategy
        for (size_t i = 0; i < mOriTruncateStrategyVec.size(); ++i) {
            if (profilePtr->mTruncateStrategyName == mOriTruncateStrategyVec[i].GetStrategyName()) {
                CompleteTruncateStrategy(profilePtr, mOriTruncateStrategyVec[i]);
            }
        }
        mTruncateProfileVec.push_back(profilePtr);
        AUTIL_LOG(INFO, "Generated a truncate profile[%s] with strategy[%s], has %lu indexes",
                  profilePtr->mTruncateProfileName.c_str(), profilePtr->mTruncateStrategyName.c_str(),
                  profilePtr->mIndexNames.size());
    }
}

void TruncateOptionConfig::Init(const IndexPartitionSchemaPtr& schema)
{
    if (mTruncateIndexConfig.size() > 0) {
        // already completed
        return;
    }

    if (schema && schema->GetTruncateProfileSchema()) {
        AUTIL_LOG(INFO, "Combine truncate config with schema");
        ProfileToStrategyNameMap profileToStrategyMap;
        InitProfileToStrategyNameMap(profileToStrategyMap);
        CompleteTruncateConfig(profileToStrategyMap, schema);
    }
    CreateTruncateIndexConfigs();
    Check(schema);
}

void TruncateOptionConfig::Check(const config::IndexPartitionSchemaPtr& schema) const
{
    for (size_t i = 0; i < mTruncateProfileVec.size(); ++i) {
        mTruncateProfileVec[i]->Check();
        const std::string& strategyName = mTruncateProfileVec[i]->mTruncateStrategyName;
        bool valid = false;
        for (size_t j = 0; j < mTruncateStrategyVec.size(); ++j) {
            if (mTruncateStrategyVec[j]->GetStrategyName() == strategyName) {
                valid = true;
                break;
            }
        }
        IE_CONFIG_ASSERT_PARAMETER_VALID(valid, "no truncate strategy for name");
    }
    for (size_t i = 0; i < mTruncateStrategyVec.size(); ++i) {
        mTruncateStrategyVec[i]->Check();
    }
    // Check that all truncate profiles under the same index use the same payload name.
    if (schema == nullptr) {
        return;
    }
    const TruncateProfileSchemaPtr& truncateProfileSchema = schema->GetTruncateProfileSchema();
    auto indexConfigs =
        schema->GetIndexSchema()->CreateIterator(/*needVirtual=*/false, /*type=*/IndexStatus::is_normal);
    for (auto indexIt = indexConfigs->Begin(); indexIt != indexConfigs->End(); indexIt++) {
        std::vector<std::string> profileNames = (*indexIt)->GetUseTruncateProfiles();
        if (profileNames.empty()) {
            continue;
        }
        std::string payloadName;
        for (const std::string& profileName : profileNames) {
            const TruncateProfileConfigPtr& truncateProfileConfig =
                truncateProfileSchema->GetTruncateProfileConfig(profileName);
            IE_CONFIG_ASSERT_PARAMETER_VALID(truncateProfileConfig != nullptr, "no truncate profile for name");
            if (truncateProfileConfig->GetPayloadConfig().IsInitialized()) {
                if (!payloadName.empty()) {
                    IE_CONFIG_ASSERT_PARAMETER_VALID(
                        payloadName == truncateProfileConfig->GetPayloadName(),
                        "all truncate profiles under the same index must use the same payload name");
                } else {
                    payloadName = truncateProfileConfig->GetPayloadName();
                }
            }
        }
    }
}

bool TruncateOptionConfig::operator==(const TruncateOptionConfig& other) const
{
    if (mOriTruncateStrategyVec.size() != other.mOriTruncateStrategyVec.size()) {
        return false;
    }

    for (size_t i = 0; i < mOriTruncateStrategyVec.size(); ++i) {
        if (!(mOriTruncateStrategyVec[i] == other.mOriTruncateStrategyVec[i])) {
            return false;
        }
    }
    return true;
}

bool TruncateOptionConfig::IsTruncateIndex(const std::string indexName) const
{
    return (mTruncateIndexConfig.find(indexName) != mTruncateIndexConfig.end());
}

void TruncateOptionConfig::CreateTruncateIndexConfigs()
{
    for (size_t i = 0; i < mTruncateProfileVec.size(); ++i) {
        const TruncateProfilePtr& truncateProfile = mTruncateProfileVec[i];
        TruncateIndexProperty property;

        property.mTruncateProfile = truncateProfile;
        property.mTruncateStrategy = GetTruncateStrategy(truncateProfile->mTruncateStrategyName);
        if (!property.mTruncateStrategy) {
            IE_CONFIG_ASSERT_PARAMETER_VALID(false,
                                             "Invalid strategy name: " + truncateProfile->mTruncateStrategyName +
                                                 ", unable to find corresponding truncate_strategy in cluster config.");
        }

        for (size_t j = 0; j < truncateProfile->mIndexNames.size(); ++j) {
            const std::string& indexName = truncateProfile->mIndexNames[j];
            property.mTruncateIndexName =
                IndexConfig::CreateTruncateIndexName(indexName, truncateProfile->mTruncateProfileName);
            TruncateIndexConfigMap::iterator iter = mTruncateIndexConfig.find(indexName);
            if (iter != mTruncateIndexConfig.end()) {
                iter->second.AddTruncateIndexProperty(property);
            } else {
                mTruncateIndexConfig[indexName].SetIndexName(indexName);
                mTruncateIndexConfig[indexName].AddTruncateIndexProperty(property);
            }
        }
    }
}

void TruncateOptionConfig::AddTruncateProfile(const TruncateProfilePtr& truncateProfile)
{
    mTruncateProfileVec.push_back(truncateProfile);
}

void TruncateOptionConfig::AddTruncateStrategy(const TruncateStrategyPtr& truncateStategy)
{
    mTruncateStrategyVec.push_back(truncateStategy);
}

const TruncateIndexConfig& TruncateOptionConfig::GetTruncateIndexConfig(const std::string& indexName) const
{
    TruncateIndexConfigMap::const_iterator iter = mTruncateIndexConfig.find(indexName);
    if (mTruncateIndexConfig.end() == iter) {
        IE_CONFIG_ASSERT_PARAMETER_VALID(false, "invalid index name");
    }
    return iter->second;
}

void TruncateOptionConfig::UpdateTruncateIndexConfig(int64_t beginTime, int64_t baseTime)
{
    for (size_t i = 0; i < mTruncateStrategyVec.size(); ++i) {
        mTruncateStrategyVec[i]->GetDiversityConstrain().ParseTimeStampFilter(beginTime, baseTime);
    }
}
}} // namespace indexlib::config
