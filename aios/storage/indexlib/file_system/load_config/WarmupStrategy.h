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

#include <string>

#include "autil/Log.h"

namespace indexlib { namespace file_system {

class WarmupStrategy
{
public:
    enum WarmupType { WARMUP_NONE, WARMUP_SEQUENTIAL };

public:
    WarmupStrategy();
    ~WarmupStrategy();

public:
    const WarmupType& GetWarmupType() const { return _warmupType; }
    void SetWarmupType(const WarmupType& warmupType) { _warmupType = warmupType; }

    bool operator==(const WarmupStrategy& warmupStrategy) const { return _warmupType == warmupStrategy._warmupType; }

public:
    static WarmupType FromTypeString(const std::string& string);
    static std::string ToTypeString(const WarmupType& warmupType);

private:
    WarmupType _warmupType;

private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<WarmupStrategy> WarmupStrategyPtr;

}} // namespace indexlib::file_system
