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

#include <cstdint>

namespace indexlibv2::index {

// 目前的过滤规则是比较确定、稳定的，不再使用virtual接口，以避免不必要的性能开销
class KKVRecordFilter final
{
public:
    KKVRecordFilter() : _ttLEnabled(false), _ttlInSec(0), _currentTimeInSec(0) {}
    KKVRecordFilter(uint64_t ttlInSec, uint64_t currentTimeInSec)
        : _ttLEnabled(true)
        , _ttlInSec(ttlInSec)
        , _currentTimeInSec(currentTimeInSec)
    {
    }

public:
    bool IsPassed(const int32_t& timestamp) const
    {
        if (_ttLEnabled) {
            return timestamp + _ttlInSec >= _currentTimeInSec;
        } else {
            return true;
        }
    }

private:
    bool _ttLEnabled;
    uint64_t _ttlInSec;
    uint64_t _currentTimeInSec;
};

} // namespace indexlibv2::index
