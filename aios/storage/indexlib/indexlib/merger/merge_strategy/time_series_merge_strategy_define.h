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
#ifndef __INDEXLIB_TIME_SERIES_MERGE_STRATEGY_DEFINE_H
#define __INDEXLIB_TIME_SERIES_MERGE_STRATEGY_DEFINE_H

#include <memory>

#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/merge_strategy/strategy_config_value_creator.h"

namespace indexlib { namespace merger {
static std::string TIME_SERIES_INPUT_INTERVAL_KEY = "input-interval";
static std::string TIME_SERIES_OUTPUT_INTERVAL_KEY = "output-interval";

template <typename T>
class TimeSeriesStrategyCondition
{
public:
    TimeSeriesStrategyCondition()
        : outputInterval(std::numeric_limits<T>::max())
        , inputInterval(std::numeric_limits<T>::max())
    {
    }

    bool FromString(const std::string& configValueStr)
    {
        if (configValueStr.empty()) {
            IE_LOG(ERROR, "config value is empty");
            return false;
        }
        std::vector<std::string> configValues;
        autil::StringUtil::fromString(configValueStr, configValues, ",");
        for (std::string& configValue : configValues) {
            std::vector<std::string> keyValue;
            autil::StringUtil::fromString(configValue, keyValue, "=");
            if (keyValue.size() != 2) {
                IE_LOG(ERROR, "keyvalue size not equal 2, keyvalue is %s", configValue.c_str());
                return false;
            }
            autil::StringUtil::trim(keyValue[0]);
            autil::StringUtil::trim(keyValue[1]);
            if (keyValue[0] == TIME_SERIES_INPUT_INTERVAL_KEY) {
                if (!autil::StringUtil::fromString(keyValue[1], inputInterval)) {
                    IE_LOG(ERROR, "parse input_interval fail %s", keyValue[1].c_str());
                    return false;
                }
            } else if (keyValue[0] == TIME_SERIES_OUTPUT_INTERVAL_KEY) {
                if (!autil::StringUtil::fromString(keyValue[1], outputInterval)) {
                    IE_LOG(ERROR, "parse output_interval fail %s", keyValue[1].c_str());
                    return false;
                }
            }

            else {
                StrategyConfigValuePtr configValue(StrategyConfigValueCreator::CreateConfigValue<T>(keyValue[1]));
                if (unlikely(configValue == NULL)) {
                    IE_LOG(ERROR, "parse config fail %s", keyValue[1].c_str());
                    return false;
                }
                conditions[keyValue[0]] = configValue;
            }
        }
        return true;
    }

    std::string ToString() const
    {
        std::stringstream ss;
        for (auto iter = conditions.begin(); iter != conditions.end(); iter++) {
            ss << iter->first << "=" << iter->second->ToString() << ",";
        }
        ss << TIME_SERIES_OUTPUT_INTERVAL_KEY << "=" << outputInterval;
        ss << TIME_SERIES_INPUT_INTERVAL_KEY << "=" << inputInterval;
        return ss.str();
    }

public:
    std::map<std::string, StrategyConfigValuePtr> conditions;
    T outputInterval;
    T inputInterval;

private:
    IE_LOG_DECLARE();
};
IE_LOG_SETUP_TEMPLATE(merger, TimeSeriesStrategyCondition);

struct TimeSeriesInputLimits {
public:
    TimeSeriesInputLimits()
        : maxDocCount(std::numeric_limits<uint32_t>::max())
        , maxSegmentSize(std::numeric_limits<uint32_t>::max())
    {
    }
    bool FromString(const std::string& inputStr)
    {
        std::vector<std::string> kvPairs;
        autil::StringUtil::fromString(inputStr, kvPairs, ";");
        bool isSetSortFiled = false;
        for (size_t i = 0; i < kvPairs.size(); i++) {
            std::vector<std::string> keyValue;
            autil::StringUtil::fromString(kvPairs[i], keyValue, "=");
            if (keyValue.size() != 2) {
                return false;
            }
            autil::StringUtil::trim(keyValue[0]);
            autil::StringUtil::trim(keyValue[1]);
            if (keyValue[0] == "max-doc-count") {
                if (autil::StringUtil::fromString(keyValue[1], maxDocCount)) {
                    continue;
                }
            }

            if (keyValue[0] == "max-segment-size") {
                if (autil::StringUtil::fromString(keyValue[1], maxSegmentSize)) {
                    continue;
                }
            }

            if (keyValue[0] == "sort-field") {
                sortField = keyValue[1];
                isSetSortFiled = true;
                continue;
            }
            return false;
        }
        if (!isSetSortFiled) {
            IE_LOG(ERROR, "not set sort-field");
            return false;
        }
        return true;
    }

    std::string ToString() const
    {
        std::stringstream ss;
        ss << "max-doc-count=" << maxDocCount << ";"
           << "max-segment-size=" << maxSegmentSize << ";"
           << "sort-field=" << sortField << ";";
        return ss.str();
    }

    uint32_t maxDocCount;
    uint32_t maxSegmentSize;
    std::string sortField;

private:
    IE_LOG_DECLARE();
};

struct TimeSeriesOutputLimits {
public:
    TimeSeriesOutputLimits()
        : maxMergedSegmentDocCount(std::numeric_limits<uint32_t>::max())
        , maxMergedSegmentSize(std::numeric_limits<uint64_t>::max())
    {
    }

    bool ParseUint32Param(const std::string& kvPair, std::string& keyName, uint32_t& value)
    {
        std::vector<std::string> strs;
        autil::StringUtil::fromString(kvPair, strs, "=");
        if (strs.size() != 2) {
            return false;
        }
        autil::StringUtil::trim(strs[0]);
        autil::StringUtil::trim(strs[1]);
        keyName = strs[0];
        return autil::StringUtil::fromString(strs[1], value);
    }

    bool FromString(const std::string& str)
    {
        std::vector<std::string> kvPairs;
        autil::StringUtil::fromString(str, kvPairs, ";");
        for (size_t i = 0; i < kvPairs.size(); i++) {
            std::string key;
            uint32_t value;
            if (!ParseUint32Param(kvPairs[i], key, value)) {
                return false;
            }

            if (key == "max-merged-segment-doc-count") {
                maxMergedSegmentDocCount = value;
                continue;
            }

            if (key == "max-merged-segment-size") {
                maxMergedSegmentSize = (uint64_t)value * 1024 * 1024;
                continue;
            }

            return false;
        }
        return true;
    }

    std::string ToString() const
    {
        std::stringstream ss;
        ss << "max-merged-segment-doc-count=" << maxMergedSegmentDocCount << ";"
           << "max-merged-segment-size=" << maxMergedSegmentSize << ";";
        return ss.str();
    }

    uint32_t maxMergedSegmentDocCount;
    uint64_t maxMergedSegmentSize;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::merger

#endif //__INDEXLIB_TIME_SERIES_META_BASE_MERGE_STRATEGY_DEFINE_H
