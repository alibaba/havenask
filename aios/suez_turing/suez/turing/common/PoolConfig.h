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

#include <stddef.h>
#include <stdint.h>

#include "autil/legacy/jsonizable.h"

namespace suez {
namespace turing {

static const size_t POOL_TRUNK_SIZE_DEFAULT = 8;           // 8 M
static const size_t POOL_RECYCLE_SIZE_LIMIT_DEFAULT = 100; // 100 M
static const size_t POOL_MAX_COUNT_DEFAULT = 100;          // 100

class PoolConfig : public autil::legacy::Jsonizable {
public:
    PoolConfig()
        : _poolTrunkSize(POOL_TRUNK_SIZE_DEFAULT)                // default 8M
        , _poolRecycleSizeLimit(POOL_RECYCLE_SIZE_LIMIT_DEFAULT) // defualt 100M
        , _poolMaxCount(POOL_MAX_COUNT_DEFAULT)                  // defualt 100
    {}
    PoolConfig(uint32_t poolTrunkSize, uint32_t poolRecycleSizeLimit, uint32_t poolMaxCount)
        : _poolTrunkSize(poolTrunkSize), _poolRecycleSizeLimit(poolRecycleSizeLimit), _poolMaxCount(poolMaxCount) {}
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("pool_trunk_size", _poolTrunkSize, _poolTrunkSize);
        json.Jsonize("pool_recycle_size_limit", _poolRecycleSizeLimit, _poolRecycleSizeLimit);
        json.Jsonize("pool_max_count", _poolMaxCount, _poolMaxCount);
    }

public:
    size_t _poolTrunkSize;        // M
    size_t _poolRecycleSizeLimit; // M
    size_t _poolMaxCount;
};
} // namespace turing
} // namespace suez
