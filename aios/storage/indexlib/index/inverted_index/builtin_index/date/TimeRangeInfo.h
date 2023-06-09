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

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"

namespace indexlib::file_system {
class IDirectory;
}

namespace indexlib::index {

class TimeRangeInfo : public autil::legacy::Jsonizable
{
public:
    TimeRangeInfo();
    ~TimeRangeInfo();

public:
    void Update(uint64_t value);
    uint64_t GetMinTime() const { return _minTime; }
    uint64_t GetMaxTime() const { return _maxTime; }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    Status Load(const std::shared_ptr<file_system::IDirectory>& directory);
    Status Store(const std::shared_ptr<file_system::IDirectory>& directory);
    bool IsValid() const { return _minTime <= _maxTime; }

private:
    volatile uint64_t _minTime;
    volatile uint64_t _maxTime;
    AUTIL_LOG_DECLARE();
};

inline void TimeRangeInfo::Update(uint64_t value)
{
    if (value < _minTime) {
        _minTime = value;
    }
    if (value > _maxTime) {
        _maxTime = value;
    }
}

} // namespace indexlib::index
