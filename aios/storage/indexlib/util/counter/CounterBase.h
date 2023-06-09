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

#include "autil/legacy/any.h"

namespace indexlib { namespace util {

class CounterBase
{
public:
    enum CounterType : uint32_t {
        CT_DIRECTORY = 0,
        CT_ACCUMULATIVE,
        CT_STATE,
        CT_STRING,
        CT_UNKNOWN,
    };

    enum FromJsonType : uint32_t {
        FJT_OVERWRITE = 0,
        FJT_MERGE,
        FJT_UNKNOWN,
    };

    static const std::string TYPE_META;

public:
    CounterBase(const std::string& path, CounterType type);
    virtual ~CounterBase();

public:
    virtual autil::legacy::Any ToJson() const = 0;
    virtual void FromJson(const autil::legacy::Any& any, FromJsonType fromJsonType) = 0;
    const std::string& GetPath() const { return _path; }
    CounterType GetType() const { return _type; }

protected:
    static std::string CounterTypeToStr(CounterType type);
    static CounterType StrToCounterType(const std::string& str);

protected:
    const std::string _path;
    const CounterType _type;
};

typedef std::shared_ptr<CounterBase> CounterBasePtr;

inline std::string CounterBase::CounterTypeToStr(CounterType type)
{
    switch (type) {
    case CT_DIRECTORY:
        return "DIR";
    case CT_ACCUMULATIVE:
        return "ACC";
    case CT_STATE:
        return "STATE";
    case CT_STRING:
        return "STRING";
    default:
        return "UNKNOWN";
    }
}

inline CounterBase::CounterType CounterBase::StrToCounterType(const std::string& str)
{
    if (str == "DIR") {
        return CT_DIRECTORY;
    } else if (str == "ACC") {
        return CT_ACCUMULATIVE;
    } else if (str == "STATE") {
        return CT_STATE;
    } else if (str == "STRING") {
        return CT_STRING;
    }
    return CT_UNKNOWN;
}
}} // namespace indexlib::util
