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

#include <limits>
#include <memory>
#include <sstream>
#include <stddef.h>
#include <string>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

namespace indexlib { namespace merger {

class StrategyConfigValue;

DEFINE_SHARED_PTR(StrategyConfigValue);

class StrategyConfigValue
{
public:
    enum ConfigValueType { RangeType, StringType };

public:
    StrategyConfigValue() {}
    virtual ~StrategyConfigValue() {}
    virtual bool Compare(const StrategyConfigValuePtr& configValue) = 0;
    virtual ConfigValueType GetType() = 0;
    virtual std::string ToString() const = 0;
};

template <typename T>
class RangeStrategyConfigValue : public StrategyConfigValue
{
public:
    RangeStrategyConfigValue() : mMax(std::numeric_limits<T>::max()), mMin(std::numeric_limits<T>::min()) {}

    RangeStrategyConfigValue(T timeMax, T timeMin) : mMax(timeMax), mMin(timeMin) {}

    ~RangeStrategyConfigValue() {}
    bool Compare(const StrategyConfigValuePtr& configValue) override
    {
        if (unlikely(configValue == NULL)) {
            return false;
        }
        RangeStrategyConfigValue* rangeStrategyConfigValue = dynamic_cast<RangeStrategyConfigValue*>(configValue.get());
        if (unlikely(rangeStrategyConfigValue == NULL)) {
            return false;
        }
        return mMin >= rangeStrategyConfigValue->mMin && mMin <= rangeStrategyConfigValue->mMax;
    }

    std::string ToString() const override
    {
        std::stringstream ss;
        ss << "[" << mMin << "," << mMax << "]";
        return ss.str();
    }

    ConfigValueType GetType() override { return RangeType; }

public:
    T mMax;
    T mMin;
};

class StringStrategyConfigValue : public StrategyConfigValue
{
public:
    StringStrategyConfigValue() {}

    StringStrategyConfigValue(std::string typeValue) : mValue(typeValue) {}

    ~StringStrategyConfigValue() {}
    bool Compare(const StrategyConfigValuePtr& configValue) override
    {
        if (unlikely(configValue == NULL)) {
            return false;
        }
        StringStrategyConfigValue* stringStrategyConfigValue =
            dynamic_cast<StringStrategyConfigValue*>(configValue.get());
        if (unlikely(stringStrategyConfigValue == NULL)) {
            return false;
        }
        return mValue == stringStrategyConfigValue->mValue;
    }
    std::string ToString() const override { return mValue; }
    ConfigValueType GetType() override { return StringType; }

public:
    std::string mValue;
};

class StrategyConfigValueCreator
{
public:
    StrategyConfigValueCreator();
    ~StrategyConfigValueCreator();

public:
    template <typename T>
    static StrategyConfigValue* CreateConfigValue(std::string configValueStr)
    {
        autil::StringUtil::trim(configValueStr);
        size_t configValueSize = configValueStr.size();
        if (configValueSize >= 2 && configValueStr[0] == '[' && configValueStr[configValueSize - 1] == ']') {
            // type range
            std::vector<std::string> rangeInfo;
            autil::StringUtil::replace(configValueStr, '[', ' ');
            autil::StringUtil::replace(configValueStr, ']', ' ');
            autil::StringUtil::fromString(configValueStr, rangeInfo, "-");
            if (unlikely(rangeInfo.size() != 2)) {
                IE_LOG(ERROR, "range keyvalue size not equal 2, keyvalue is [%s]", configValueStr.c_str());
                return NULL;
            }
            T min = std::numeric_limits<T>::min();
            T max = std::numeric_limits<T>::max();
            autil::StringUtil::trim(rangeInfo[0]);
            autil::StringUtil::trim(rangeInfo[1]);
            if (rangeInfo[0] != "") {
                if (unlikely(!autil::StringUtil::fromString(rangeInfo[0], min))) {
                    IE_LOG(ERROR, "parse range min value fail, min value is [%s]", rangeInfo[0].c_str());
                    return NULL;
                }
            }
            if (rangeInfo[1] != "") {
                if (unlikely(!autil::StringUtil::fromString(rangeInfo[1], max))) {
                    IE_LOG(ERROR, "parse range max value fail, max value is [%s]", rangeInfo[1].c_str());
                    return NULL;
                }
            }
            if (min > max) {
                IE_LOG(ERROR, "parse range fail, min is greater than max,  min is [%s], max is [%s]",
                       rangeInfo[0].c_str(), rangeInfo[1].c_str());
                return NULL;
            }
            RangeStrategyConfigValue<T>* rangeConfigValue = new RangeStrategyConfigValue<T>(max, min);
            return rangeConfigValue;
        } else {
            if (unlikely(configValueStr.empty())) {
                IE_LOG(ERROR, "parse string config fail, value is empty");
                return NULL;
            }
            StringStrategyConfigValue* stringConfigValue = new StringStrategyConfigValue(configValueStr);
            return stringConfigValue;
        }
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(StrategyConfigValueCreator);
}} // namespace indexlib::merger
