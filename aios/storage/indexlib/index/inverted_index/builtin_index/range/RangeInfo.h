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
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"

namespace indexlib::file_system {
class IDirectory;
}

namespace indexlib::index {

class RangeInfo : public autil::legacy::Jsonizable
{
public:
    RangeInfo();
    ~RangeInfo();

public:
    void Update(uint64_t value);
    uint64_t GetMinNumber() const { return _minNumber; }
    uint64_t GetMaxNumber() const { return _maxNumber; }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    Status Load(const std::shared_ptr<file_system::IDirectory>& directory);
    Status Store(const std::shared_ptr<file_system::IDirectory>& directory);
    bool IsValid() const { return _minNumber <= _maxNumber; }

private:
    volatile uint64_t _minNumber;
    volatile uint64_t _maxNumber;

    AUTIL_LOG_DECLARE();
};

inline void RangeInfo::Update(uint64_t value)
{
    if (value < _minNumber) {
        _minNumber = value;
    }
    if (value > _maxNumber) {
        _maxNumber = value;
    }
}
} // namespace indexlib::index
