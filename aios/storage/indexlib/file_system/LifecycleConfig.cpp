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
#include "indexlib/file_system/LifecycleConfig.h"

#include <algorithm>
#include <ext/alloc_traits.h>
#include <stddef.h>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "indexlib/util/Exception.h"

namespace indexlib::file_system {

AUTIL_LOG_SETUP(indexlib.file_system, LifecycleConfig);

IntegerLifecyclePattern::IntegerLifecyclePattern() : LifecyclePatternBase(false) {}

IntegerLifecyclePattern::~IntegerLifecyclePattern() {}

void IntegerLifecyclePattern::Jsonize(JsonWrapper& json)
{
    json.Jsonize("lifecycle", lifecycle, lifecycle);
    json.Jsonize("range", range, range);
    json.Jsonize("statistic_field", statisticField, statisticField);
    json.Jsonize("statistic_type", statisticType, statisticType);
    json.Jsonize("offset_base", offsetBase, offsetBase);
    json.Jsonize("base_value", baseValue, baseValue);
    json.Jsonize("is_offset", isOffset, isOffset);
}

bool IntegerLifecyclePattern::InitOffsetBase(const std::map<std::string, std::string>& arguments)
{
    if (offsetBase.empty()) {
        baseValue = 0;
        return true;
    }
    std::string valueStr = offsetBase;
    auto it = arguments.find(offsetBase);
    if (it != arguments.end()) {
        valueStr = it->second;
    }
    int64_t value;
    if (!autil::StringUtil::numberFromString(valueStr, value)) {
        return false;
    }
    baseValue = value;
    return true;
}

StringLifecyclePattern::StringLifecyclePattern() : LifecyclePatternBase(false) {}
StringLifecyclePattern::~StringLifecyclePattern() {}

void StringLifecyclePattern::Jsonize(JsonWrapper& json)
{
    json.Jsonize("lifecycle", lifecycle, lifecycle);
    json.Jsonize("range", range, range);
    json.Jsonize("statistic_field", statisticField, statisticField);
    json.Jsonize("statistic_type", statisticType, statisticType);
}

std::vector<std::shared_ptr<LifecyclePatternBase>> Create(const autil::legacy::Any& patternsAny)
{
    std::vector<std::shared_ptr<LifecyclePatternBase>> patterns;
    std::vector<autil::legacy::Any> patternsVec = autil::legacy::AnyCast<std::vector<autil::legacy::Any>>(patternsAny);
    for (const auto& patternAny : patternsVec) {
        std::shared_ptr<LifecyclePatternBase> pattern;
        auto& jsonMap = autil::legacy::AnyCast<autil::legacy::json::JsonMap>(patternAny);
        auto typeIter = jsonMap.find("statistic_type");
        if (typeIter == jsonMap.end()) {
            INDEXLIB_THROW(util::BadParameterException, "can not find key statistic_type in json");
        }
        std::string type = autil::legacy::AnyCast<std::string>(typeIter->second);
        if (type == LifecyclePatternBase::INTEGER_TYPE) {
            pattern.reset(new IntegerLifecyclePattern());
        } else if (type == LifecyclePatternBase::STRING_TYPE) {
            pattern.reset(new StringLifecyclePattern());
        } else {
            INDEXLIB_THROW(util::BadParameterException, "unsupport type [%s]", type.c_str());
        }
        autil::legacy::Jsonizable::JsonWrapper jsonWrapper(patternAny);
        pattern->Jsonize(jsonWrapper);
        patterns.push_back(pattern);
    }
    return patterns;
}

struct LifecycleConfig::LifecycleConfigImpl : public autil::legacy::Jsonizable {
    LifecycleConfigImpl() : enableLocalDeployManifest(false), strategy(LifecycleConfig::STATIC_STRATEGY) {}
    ~LifecycleConfigImpl() {}

    bool enableLocalDeployManifest;
    std::string strategy;
    std::vector<std::shared_ptr<LifecyclePatternBase>> patterns;

    bool operator==(const LifecycleConfigImpl& other) const
    {
        if (enableLocalDeployManifest != other.enableLocalDeployManifest || strategy != other.strategy ||
            patterns == other.patterns) {
            return false;
        }
        return true;
    }

    void Jsonize(JsonWrapper& json) override
    {
        json.Jsonize("strategy", strategy, strategy);
        if (json.GetMode() == FROM_JSON) {
            auto jsonMap = json.GetMap();
            auto patternsIter = jsonMap.find("patterns");
            if (patternsIter != jsonMap.end()) {
                patterns = Create(patternsIter->second);
            }
            bool defaultValueForDeployManifest = (strategy == DYNAMIC_STRATEGY);
            json.Jsonize("enable_local_deploy_manifest", enableLocalDeployManifest, defaultValueForDeployManifest);
            if (strategy == DYNAMIC_STRATEGY && enableLocalDeployManifest == false) {
                AUTIL_LOG(WARN, "attention: enable_local_deploy_manifest = false"
                                "in dynamic lifecycle strategy");
            }
            if (!Validate()) {
                INDEXLIB_THROW(util::BadParameterException, "invalid patterns in LifecycleConfig");
            }

        } else {
            std::vector<autil::legacy::Any> values;
            for (const auto& pattern : patterns) {
                values.push_back(ToJson(*pattern));
            }
            json.Jsonize("patterns", values, values);
            json.Jsonize("enable_local_deploy_manifest", enableLocalDeployManifest);
        }
    }

    bool Validate() const
    {
        if (strategy != DYNAMIC_STRATEGY && strategy != STATIC_STRATEGY) {
            AUTIL_LOG(ERROR, "invalid lifecyle strategy[%s]", strategy.c_str());
            return false;
        }
        if (strategy == STATIC_STRATEGY) {
            for (size_t i = 0; i < patterns.size(); i++) {
                if (true == patterns[i]->isOffset) {
                    AUTIL_LOG(ERROR, "static strategy not support offset-based pattern[%zu]", i);
                    return false;
                }
            }
        }
        return true;
    }

    bool EnableLocalDeployManifestChecking() const { return enableLocalDeployManifest; }
    bool InitOffsetBase(const std::map<std::string, std::string>& arguments)
    {
        for (auto pattern : patterns) {
            if (!pattern->InitOffsetBase(arguments)) {
                AUTIL_LOG(ERROR, "init offset base with arg [%s] failed",
                          autil::legacy::ToJsonString(arguments, true).c_str());
                return false;
            }
        }
        return true;
    }
};

const std::string LifecycleConfig::STATIC_STRATEGY = "static";
const std::string LifecycleConfig::DYNAMIC_STRATEGY = "dynamic";
const std::string LifecyclePatternBase::INTEGER_TYPE = "integer";
const std::string LifecyclePatternBase::DOUBLE_TYPE = "double";
const std::string LifecyclePatternBase::STRING_TYPE = "string";
const std::string LifecyclePatternBase::CURRENT_TIME = "CURRENT_TIME";

int64_t LifecycleConfig::_TEST_currentTimeBase = -1;
LifecycleConfig::LifecycleConfig() : _impl(std::make_unique<LifecycleConfigImpl>()) {}
LifecycleConfig::LifecycleConfig(const LifecycleConfig& other)
    : _impl(std::make_unique<LifecycleConfigImpl>(*other._impl))
{
}

LifecycleConfig& LifecycleConfig::operator=(const LifecycleConfig& other)
{
    if (this != &other) {
        _impl = std::make_unique<LifecycleConfigImpl>(*other._impl);
    }
    return *this;
}

bool LifecycleConfig::operator==(const LifecycleConfig& other) const { return *_impl == *other._impl; }
bool LifecycleConfig::operator!=(const LifecycleConfig& other) const { return !(*_impl == *other._impl); }

LifecycleConfig::~LifecycleConfig() {}

void LifecycleConfig::Jsonize(JsonWrapper& json) { _impl->Jsonize(json); }
const std::string& LifecycleConfig::GetStrategy() const { return _impl->strategy; }

const std::vector<std::shared_ptr<LifecyclePatternBase>>& LifecycleConfig::GetPatterns() const
{
    return _impl->patterns;
}

bool LifecycleConfig::IsRangeOverLapped(const IntegerRangeType& lhs, const IntegerRangeType& rhs)
{
    if ((lhs.first >= rhs.first && lhs.first < rhs.second) || (rhs.first >= lhs.first && rhs.first < lhs.second)) {
        return true;
    }
    return false;
}

bool LifecycleConfig::InitOffsetBase(const std::map<std::string, std::string>& arguments)
{
    return _impl->InitOffsetBase(arguments);
}

bool LifecycleConfig::EnableLocalDeployManifestChecking() const { return _impl->EnableLocalDeployManifestChecking(); }

int64_t LifecycleConfig::CurrentTimeInSeconds()
{
    if (_TEST_currentTimeBase == -1) {
        return autil::TimeUtility::currentTimeInSeconds();
    }
    return _TEST_currentTimeBase;
}

void LifecycleConfig::UpdateCurrentTimeBase(int64_t time) { _TEST_currentTimeBase = time; }

} // namespace indexlib::file_system
