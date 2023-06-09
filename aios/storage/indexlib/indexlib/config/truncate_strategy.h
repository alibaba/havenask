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
#ifndef __INDEXLIB_TRUNCATE_STRATEGY_CONFIG_H
#define __INDEXLIB_TRUNCATE_STRATEGY_CONFIG_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/config_define.h"
#include "indexlib/config/diversity_constrain.h"
#include "indexlib/config/meta_re_truncate_config.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace config {

class TruncateStrategy : public autil::legacy::Jsonizable
{
public:
    TruncateStrategy();
    ~TruncateStrategy();

public:
    const std::string& GetStrategyName() const { return mStrategyName; }
    const std::string& GetStrategyType() const { return mStrategyType; }
    std::vector<std::string>& GetProfileNames() { return mTruncateProfileNames; }
    uint64_t GetThreshold() const { return mThreshold; }
    int32_t GetMemoryOptimizeThreshold() const { return mMemoryOptimizeThreshold; }
    uint64_t GetLimit() const { return mLimit; }
    const DiversityConstrain& GetDiversityConstrain() const { return mDiversityConstrain; }

    DiversityConstrain& GetDiversityConstrain() { return mDiversityConstrain; }

    void SetStrategyName(const std::string& strategyName) { mStrategyName = strategyName; }

    void SetStrategyType(const std::string& strategyType) { mStrategyType = strategyType; }
    void SetThreshold(uint64_t threshold) { mThreshold = threshold; }
    void SetMemoryOptimizeThreshold(int32_t threshold) { mMemoryOptimizeThreshold = threshold; }
    void SetLimit(uint64_t limit) { mLimit = limit; }
    void SetDiversityConstrain(const DiversityConstrain& divCons) { mDiversityConstrain = divCons; }
    // for d2 compitable
    void AddProfileName(const std::string& profileName) { mTruncateProfileNames.push_back(profileName); }

    bool HasLimit() const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;
    bool operator==(const TruncateStrategy& other) const;

    const MetaReTruncateConfig& GetReTruncateConfig() const { return mReTruncateConfig; }

public:
    MetaReTruncateConfig& TEST_GetReTruncateConfig() { return mReTruncateConfig; }

private:
    static const uint64_t MAX_TRUNC_THRESHOLD;
    static const uint64_t MAX_TRUNC_LIMIT;
    static const int32_t MAX_MEMORY_OPTIMIZE_PERCENTAGE;
    static const int32_t DEFAULT_MEMORY_OPTIMIZE_PERCENTAGE;

private:
    std::string mStrategyName;
    std::string mStrategyType;
    std::vector<std::string> mTruncateProfileNames;
    uint64_t mThreshold;
    int32_t mMemoryOptimizeThreshold;
    uint64_t mLimit;
    DiversityConstrain mDiversityConstrain;

    // workaround for mainse full merge, will deprecate in the future
    MetaReTruncateConfig mReTruncateConfig;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateStrategy);
}} // namespace indexlib::config

#endif //__INDEXLIB_TRUNCATE_STRATEGY_CONFIG_H
