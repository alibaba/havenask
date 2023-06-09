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
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"

#include <assert.h>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <unistd.h>

#include "indexlib/file_system/load_config/MemLoadStrategy.h"
#include "indexlib/util/Exception.h"

using namespace std;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, MmapLoadStrategy);

MmapLoadStrategy::MmapLoadStrategy()
    : _isLock(false)
    , _adviseRandom(false)
    , _slice(4 * 1024 * 1024) // 4M
    , _interval(0)
{
}

MmapLoadStrategy::MmapLoadStrategy(bool isLock, bool adviseRandom, uint32_t slice, uint32_t interval)
    : _isLock(isLock)
    , _adviseRandom(adviseRandom)
    , _slice(slice)
    , _interval(interval)
{
}

MmapLoadStrategy::~MmapLoadStrategy() {}

MemLoadStrategy* MmapLoadStrategy::CreateMemLoadStrategy() const noexcept
{
    return new MemLoadStrategy(_slice, _interval, _enableLoadSpeedLimit);
}

uint32_t MmapLoadStrategy::GetInterval() const
{
    if (!_enableLoadSpeedLimit || *_enableLoadSpeedLimit) {
        return _interval;
    }
    return 0;
}

void MmapLoadStrategy::Check()
{
    if (_slice == 0) {
        INDEXLIB_THROW(util::BadParameterException, "slice can not be 0");
    }
    if (_slice % getpagesize() != 0) {
        INDEXLIB_THROW(util::BadParameterException, "slice must be the multiple of page size[%d]", getpagesize());
    }
}

void MmapLoadStrategy::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("lock", _isLock, false);
    json.Jsonize("advise_random", _adviseRandom, false);
    json.Jsonize("slice", _slice, (uint32_t)(4 * 1024 * 1024));
    json.Jsonize("interval", _interval, (uint32_t)0);
}

bool MmapLoadStrategy::EqualWith(const LoadStrategyPtr& loadStrategy) const
{
    assert(loadStrategy);
    if (GetLoadStrategyName() != loadStrategy->GetLoadStrategyName()) {
        return false;
    }
    MmapLoadStrategyPtr right = std::dynamic_pointer_cast<MmapLoadStrategy>(loadStrategy);
    assert(right);

    return *this == *right;
}

bool MmapLoadStrategy::operator==(const MmapLoadStrategy& loadStrategy) const
{
    return _isLock == loadStrategy._isLock && _adviseRandom == loadStrategy._adviseRandom &&
           _slice == loadStrategy._slice && _interval == loadStrategy._interval;
}

void MmapLoadStrategy::SetEnableLoadSpeedLimit(const std::shared_ptr<bool>& enableLoadSpeedLimit)
{
    _enableLoadSpeedLimit = enableLoadSpeedLimit;
}

}} // namespace indexlib::file_system
