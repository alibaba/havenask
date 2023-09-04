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
#include "indexlib/config/IndexTaskConfig.h"

#include "autil/StringTokenizer.h"
#include "autil/legacy/json.h"
#include "indexlib/config/IndexTaskConfig.h"
#include "indexlib/legacy/config/OfflineMergeConfig.h"

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, IndexTaskConfig);
AUTIL_LOG_SETUP(indexlib.config, Trigger);

struct IndexTaskConfig::Impl {
    std::string taskName;
    std::string taskType;
    Trigger trigger;
    autil::legacy::json::JsonMap settings;
};

const std::string& Trigger::GetTriggerStr() const { return _triggerStr; }

Trigger::TriggerType Trigger::GetTriggerType() const { return _type; }
std::optional<tm> Trigger::GetTriggerDayTime() const { return _triggerDayTime; }
std::optional<int64_t> Trigger::GetTriggerPeriod() const { return _period; }

Trigger::TriggerType Trigger::Parse(const std::string& originTriggerStr)
{
    std::string triggerStr = originTriggerStr;
    autil::StringUtil::trim(triggerStr);
    autil::StringUtil::toLowerCase(triggerStr);
    if (triggerStr == MANUAL_KEY) {
        return TriggerType::MANUAL;
    }
    if (triggerStr == AUTO_KEY) {
        return TriggerType::AUTO;
    }

    autil::StringTokenizer st(triggerStr, "=",
                              autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);

    if (2 != st.getNumTokens() || (st[0] != DAYTIME_KEY && st[0] != PERIOD_KEY)) {
        AUTIL_LOG(ERROR, "invalid trigger config [%s]", originTriggerStr.c_str());
        return TriggerType::ERROR;
    }

    std::string key = st[0];
    if (key == DAYTIME_KEY) {
        std::string triggerTimeStr = st[1];
        struct tm triggerTime = {0};
        char* p = strptime(triggerTimeStr.c_str(), "%H:%M", &triggerTime);
        if (p != triggerTimeStr.c_str() + triggerTimeStr.size()) {
            AUTIL_LOG(ERROR, "fail to convert [%s] to day time, originTriggerStr is [%s]", triggerTimeStr.c_str(),
                      originTriggerStr.c_str());
            return TriggerType::ERROR;
        }
        _triggerDayTime = std::make_optional<tm>(triggerTime);
        return TriggerType::DATE_TIME;
    }

    if (key == PERIOD_KEY) {
        std::string periodStr = st[1];
        int64_t period = 0;
        if (!autil::StringUtil::fromString(periodStr, period)) {
            AUTIL_LOG(ERROR, "fail to convert [%s] to period time, originTrigger is [%s]", periodStr.c_str(),
                      originTriggerStr.c_str());
            return TriggerType::ERROR;
        }
        if (period <= 0) {
            std::string errorMsg = "period time [" + periodStr + "] illegal";
            AUTIL_LOG(ERROR, "%s", errorMsg.c_str());
            return TriggerType::ERROR;
        }
        _period = std::make_optional<int64_t>(period);
        return TriggerType::PERIOD;
    }
    return TriggerType::ERROR;
}

Trigger::Trigger(const std::string& originTriggerStr)
{
    _triggerStr = originTriggerStr;
    _type = Parse(originTriggerStr);
}

IndexTaskConfig::IndexTaskConfig() : _impl(std::make_unique<IndexTaskConfig::Impl>()) {}
IndexTaskConfig::IndexTaskConfig(const std::string& taskName, const std::string& taskType, const std::string& trigger)
    : _impl(std::make_unique<IndexTaskConfig::Impl>())
{
    _impl->taskName = taskName;
    _impl->taskType = taskType;
    _impl->trigger = trigger;
}
IndexTaskConfig::~IndexTaskConfig() {}

IndexTaskConfig::IndexTaskConfig(const IndexTaskConfig& other)
    : _impl(std::make_unique<IndexTaskConfig::Impl>(*other._impl))
{
}

IndexTaskConfig& IndexTaskConfig::operator=(const IndexTaskConfig& other)
{
    if (this != &other) {
        _impl = std::make_unique<IndexTaskConfig::Impl>(*other._impl);
    }
    return *this;
}

const std::string& IndexTaskConfig::GetTaskName() const { return _impl->taskName; }
const std::string& IndexTaskConfig::GetTaskType() const { return _impl->taskType; }
const Trigger& IndexTaskConfig::GetTrigger() const { return _impl->trigger; }
std::pair<bool, const autil::legacy::Any&> IndexTaskConfig::GetSettingConfig(const std::string& key) const
{
    static autil::legacy::Any empty;
    auto iter = _impl->settings.find(key);
    if (iter != _impl->settings.end()) {
        return {true, iter->second};
    }
    return {false, empty};
}

void IndexTaskConfig::FillLegacyReclaimConfig(const std::string& name,
                                              const indexlib::legacy::config::OfflineMergeConfig& legacyMergeConfig)
{
    _impl->taskName = name + "_legacy_reclaim";
    _impl->taskType = "reclaim";
    std::string period = legacyMergeConfig.periodMergeDescription;
    if (!period.empty()) {
        _impl->trigger = Trigger(legacyMergeConfig.periodMergeDescription);
    } else {
        _impl->trigger = Trigger("manual");
    }

    auto iter = legacyMergeConfig.reclaimConfigs.find("doc_reclaim_source");
    assert(iter != legacyMergeConfig.reclaimConfigs.end());
    _impl->settings["doc_reclaim_source"] = iter->second;
    AUTIL_LOG(INFO, "fill legacy reclaim config name [%s], type [%s]", _impl->taskName.c_str(),
              _impl->taskType.c_str());
}

void IndexTaskConfig::FillMergeConfig(const std::string& name, const indexlibv2::config::MergeConfig& mergeConfig)
{
    _impl->taskName = name;
    _impl->taskType = "merge";
    autil::legacy::json::JsonMap settings;
    settings["merge_config"] = autil::legacy::ToJson(mergeConfig);
    _impl->settings = settings;
}

void IndexTaskConfig::FillLegacyMergeConfig(const std::string& name,
                                            const indexlib::legacy::config::OfflineMergeConfig& legacyMergeConfig)
{
    _impl->taskName = (name == "full") ? "full_merge" : name;
    _impl->taskType = "merge";
    std::string period = legacyMergeConfig.periodMergeDescription;
    if (!period.empty()) {
        _impl->trigger = Trigger(legacyMergeConfig.periodMergeDescription);
    } else {
        _impl->trigger = Trigger("manual");
    }

    autil::legacy::json::JsonMap settings;
    auto parallelNum = legacyMergeConfig.mergeParallelNum;
    if (parallelNum > 0) {
        settings["parallel_num"] = parallelNum;
    }
    indexlibv2::config::MergeConfig mergeConfig;
    autil::legacy::FromJson(mergeConfig, legacyMergeConfig.mergeConfig);
    auto threadCount = mergeConfig.GetMergeThreadCount();
    if (threadCount > 0) {
        settings["thread_num"] = threadCount;
    }
    settings["merge_config"] = legacyMergeConfig.mergeConfig;
    _impl->settings = settings;
    AUTIL_LOG(INFO, "fill legacy merge config [%s].[%s] with parallelNum [%d] threadNum [%d]", _impl->taskType.c_str(),
              _impl->taskName.c_str(), parallelNum, threadCount);
}

void IndexTaskConfig::RewriteWithDefaultMergeConfig(const indexlibv2::config::MergeConfig& defaultMergeConfig)
{
    auto mergeConfig = defaultMergeConfig;
    auto [isExist, settingAny] = GetSettingConfig("merge_config");
    if (isExist) {
        autil::legacy::FromJson(mergeConfig, settingAny);
    }
    _impl->settings["merge_config"] = autil::legacy::ToJson(mergeConfig);
    auto [status, threadCount] = GetSetting<uint32_t>("thread_num");
    if (!status.IsOK()) {
        _impl->settings["thread_num"] = mergeConfig.GetMergeThreadCount();
    }
}

void IndexTaskConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("task_name", _impl->taskName, _impl->taskName);
    json.Jsonize("task_type", _impl->taskType, _impl->taskType);

    std::string triggerStr = _impl->trigger.GetTriggerStr();
    json.Jsonize("trigger", triggerStr);
    if (json.GetMode() == autil::legacy::Jsonizable::FROM_JSON) {
        _impl->trigger = Trigger(triggerStr);
    }
    json.Jsonize("settings", _impl->settings, _impl->settings);
}

} // namespace indexlibv2::config
