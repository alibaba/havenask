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
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/truncate_index_config.h"
#include "indexlib/config/truncate_profile.h"
#include "indexlib/config/truncate_strategy.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib { namespace config {

typedef std::vector<std::string> StringVec;
typedef std::map<std::string, std::string> ProfileToStrategyNameMap;
typedef std::map<std::string, TruncateIndexConfig> TruncateIndexConfigMap;
typedef std::vector<TruncateStrategyPtr> TruncateStrategyVec;
typedef std::vector<TruncateProfilePtr> TruncateProfileVec;

// todo: remove inherited from Jsonizable
class TruncateOptionConfig : public autil::legacy::Jsonizable
{
public:
    // for TruncateConfigMaker
    TruncateOptionConfig() {}

    TruncateOptionConfig(const std::vector<TruncateStrategy>& truncateStrategyVec);
    ~TruncateOptionConfig();

public:
    // TODO: remove later
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {}

    void Check(const config::IndexPartitionSchemaPtr& schema) const;

    bool operator==(const TruncateOptionConfig& other) const;

    bool IsTruncateIndex(const std::string indexName) const;

    const TruncateIndexConfig& GetTruncateIndexConfig(const std::string& indexName) const;

    const TruncateProfileVec& GetTruncateProfiles() const { return mTruncateProfileVec; }

    const TruncateProfilePtr& GetTruncateProfile(const std::string& profileName) const;

    const TruncateStrategyVec& GetTruncateStrategys() const { return mTruncateStrategyVec; }

    const TruncateStrategyPtr& GetTruncateStrategy(const std::string& strategyName) const;

    const TruncateIndexConfigMap& GetTruncateIndexConfigs() const { return mTruncateIndexConfig; }

    void Init(const IndexPartitionSchemaPtr& schema);

    void UpdateTruncateIndexConfig(int64_t beginTime, int64_t baseTime);

public:
    // for test
    void AddTruncateProfile(const TruncateProfilePtr& truncateProfile);
    void AddTruncateStrategy(const TruncateStrategyPtr& truncateStategy);
    const std::vector<TruncateStrategy>& GetOriTruncateStrategy() const { return mOriTruncateStrategyVec; }

private:
    void CompleteTruncateStrategy(TruncateProfilePtr& profilePtr, const TruncateStrategy& strategy);

    void CreateTruncateIndexConfigs();

    void CompleteTruncateConfig(ProfileToStrategyNameMap& profileToStrategyMap, const IndexPartitionSchemaPtr& schema);

    void InitProfileToStrategyNameMap(ProfileToStrategyNameMap& profileToStrategyMap);

private:
    std::vector<TruncateStrategy> mOriTruncateStrategyVec;
    TruncateProfileVec mTruncateProfileVec;
    TruncateStrategyVec mTruncateStrategyVec;
    TruncateIndexConfigMap mTruncateIndexConfig;
    TruncateStrategyPtr mDefaultTruncateStrategyPtr;
    TruncateProfilePtr mDefaultTruncateProfilePtr;

private:
    AUTIL_LOG_DECLARE();
    friend class TruncateOptionConfigTest;
};

typedef std::shared_ptr<TruncateOptionConfig> TruncateOptionConfigPtr;
}} // namespace indexlib::config
