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

#include <bits/types/struct_tm.h>
#include <cstdint>
#include <ctime>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "autil/Log.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/MergeConfig.h"

namespace indexlib { namespace legacy { namespace config {
class OfflineMergeConfig;
}}} // namespace indexlib::legacy::config

namespace indexlibv2::config {
/*
  example:
        "index_task_configs" : [
            {
                "task_type" : "merge",
                "task_name" : "merge_inc",
                "trigger" : "auto",
                "settings" : {
                    "merge_config": {
                         "merge_strategy" : "optimize",
                         "merge_strategy_param" : ""
                    }
                }
            },
            {
                "task_type" : "reclaim",
                "task_name" : "reclaim_index",
                "trigger" : "period=300"
            }
        ]

    trigger 支持4种模式："trigger" : "manual"，"trigger" : "auto",  "trigger" : "period=300",  "trigger" :
    "daytime=1:00"
    auto代表每轮都可以执行，manual除非上层或手动指定，否则不会自动执行，"period=300"代表距离上次执行过去300s后可以执行，
    daytime代表超过时间点可以执行一一次.
*/

class Trigger
{
public:
    enum TriggerType {
        ERROR,
        MANUAL,
        AUTO,
        PERIOD,
        DATE_TIME,
    };

    Trigger() = default;
    ~Trigger() = default;
    Trigger(const std::string& triggerStr);
    Trigger(const Trigger& other) = default;
    Trigger& operator=(const Trigger& other) = default;

    const std::string& GetTriggerStr() const;
    TriggerType GetTriggerType() const;
    std::optional<tm> GetTriggerDayTime() const;
    std::optional<int64_t> GetTriggerPeriod() const;

private:
    TriggerType Parse(const std::string& originTriggerStr);

private:
    std::string _triggerStr;
    TriggerType _type = TriggerType::ERROR;
    std::optional<int64_t> _period;
    std::optional<tm> _triggerDayTime;

private:
    AUTIL_LOG_DECLARE();
};

class IndexTaskConfig : public autil::legacy::Jsonizable
{
public:
    IndexTaskConfig();
    IndexTaskConfig(const std::string& taskName, const std::string& taskType, const std::string& period);
    ~IndexTaskConfig();
    IndexTaskConfig(const IndexTaskConfig& other);
    IndexTaskConfig& operator=(const IndexTaskConfig& other);

public:
    const std::string& GetTaskName() const;
    const std::string& GetTaskType() const;
    const Trigger& GetTrigger() const;

    template <typename T>
    std::pair<Status, T> GetSetting(const std::string& key) const;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    void FillMergeConfig(const std::string& name, const indexlibv2::config::MergeConfig& mergeConfig);
    void RewriteWithDefaultMergeConfig(const indexlibv2::config::MergeConfig& mergeConfig);
    void FillLegacyMergeConfig(const std::string& name,
                               const indexlib::legacy::config::OfflineMergeConfig& mergeConfig);

    void FillLegacyReclaimConfig(const std::string& name,
                                 const indexlib::legacy::config::OfflineMergeConfig& legacyMergeConfig);

public:
    Status TEST_SetSetting(const std::string& key, const std::string& str) const;

private:
    std::pair<bool, const autil::legacy::Any&> GetSettingConfig(const std::string& key) const;

private:
    struct Impl;

    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
std::pair<Status, T> IndexTaskConfig::GetSetting(const std::string& key) const
{
    T setting {};
    auto [isExist, settingAny] = GetSettingConfig(key);
    if (!isExist) {
        return {Status::NotFound(), setting};
    }
    try {
        autil::legacy::FromJson(setting, settingAny);
    } catch (const autil::legacy::ExceptionBase& e) {
        Status status = Status::ConfigError("[%s] is json invalid, %s", key.c_str(), e.what());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return {status, setting};
    }
    return {Status::OK(), setting};
}

static constexpr const char* DAYTIME_KEY = "daytime";
static constexpr const char* PERIOD_KEY = "period";
static constexpr const char* AUTO_KEY = "auto";
static constexpr const char* MANUAL_KEY = "manual";

} // namespace indexlibv2::config
