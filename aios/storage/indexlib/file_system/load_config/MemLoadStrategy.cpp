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
#include "indexlib/file_system/load_config/MemLoadStrategy.h"

#include <assert.h>
#include <iosfwd>
#include <memory>
#include <unistd.h>

#include "indexlib/util/Exception.h"

using namespace std;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, MemLoadStrategy);

MemLoadStrategy::MemLoadStrategy(uint32_t slice, uint32_t interval, const std::shared_ptr<bool>& enableLoadSpeedLimit)
    : _slice(slice)
    , _interval(interval)
    , _enableLoadSpeedLimit(enableLoadSpeedLimit)
{
}

MemLoadStrategy::~MemLoadStrategy() {}

void MemLoadStrategy::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {}

bool MemLoadStrategy::EqualWith(const LoadStrategyPtr& loadStrategy) const
{
    assert(loadStrategy);
    if (GetLoadStrategyName() != loadStrategy->GetLoadStrategyName()) {
        return false;
    }
    auto right = std::dynamic_pointer_cast<MemLoadStrategy>(loadStrategy);
    assert(right);

    return _slice == right->_slice && _interval == right->_interval;
}

void MemLoadStrategy::Check()
{
    if (_slice == 0) {
        INDEXLIB_THROW(util::BadParameterException, "slice can not be 0");
    }
    if (_slice % getpagesize() != 0) {
        INDEXLIB_THROW(util::BadParameterException, "slice must be the multiple of page size[%d]", getpagesize());
    }
}

uint32_t MemLoadStrategy::GetInterval() const
{
    if (!_enableLoadSpeedLimit || _enableLoadSpeedLimit) {
        return _interval;
    }
    return 0;
}
}} // namespace indexlib::file_system
