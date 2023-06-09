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

#include <stdint.h>
#include <stdlib.h>
#include <vector>

#include "cava/common/Common.h"

class CavaCtx;

namespace unsafe {
class Any;

class AggItemAllocator
{
public:
    AggItemAllocator(uint32_t itemSize)
        : _currentTrunk(NULL)
        , _currentOffset(0)
        , _itemSize(itemSize)

    {}
    ~AggItemAllocator() {
        for (auto trunk : _trunks) {
            free(trunk);
        }
        _trunks.clear();
    }
public:
    static AggItemAllocator *create(CavaCtx *ctx, uint itemSize);
    unsafe::Any *alloc(CavaCtx *ctx);
private:
    bool allocTrunk();
private:
    void *_currentTrunk;
    uint32_t _currentOffset;
    uint32_t _itemSize;
    std::vector<void *> _trunks;
private:
    static const uint32_t TRUNK_SIZE = 1024;
    static const size_t DEFAULT_ALIGN_SIZE = 4;
};

}
