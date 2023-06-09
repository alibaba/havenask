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

#include "autil/Lock.h"
#include "autil/Log.h"
#include "indexlib/util/counter/Counter.h"

namespace indexlib { namespace util {

class StringCounter : public CounterBase
{
public:
    StringCounter(const std::string& path);
    ~StringCounter();

private:
    StringCounter(const StringCounter&) = delete;
    StringCounter& operator=(const StringCounter&) = delete;
    StringCounter(StringCounter&&) = delete;
    StringCounter& operator=(StringCounter&&) = delete;

public:
    autil::legacy::Any ToJson() const override;
    void FromJson(const autil::legacy::Any& any, FromJsonType fromJsonType) override;

    void Set(const std::string& value);
    std::string Get() const;

private:
    mutable autil::ThreadMutex _mutex;
    std::string _value;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<StringCounter> StringCounterPtr;
}} // namespace indexlib::util
