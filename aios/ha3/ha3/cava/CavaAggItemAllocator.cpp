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
#include <stdlib.h>
#include <iosfwd>
#include <vector>

#include "autil/CommonMacros.h"

#include "ha3/cava/AggItemAllocator.h"

class CavaCtx;
namespace unsafe {
class Any;
}  // namespace unsafe

using namespace std;

namespace unsafe {

unsafe::Any *AggItemAllocator::alloc(CavaCtx *ctx) {
    void *ret = NULL;
    if (_currentOffset >= TRUNK_SIZE * _itemSize || _currentTrunk == NULL) {
        if (!allocTrunk()) {
            return (unsafe::Any *)ret;
        }
    }
    ret = (char *)_currentTrunk + _currentOffset;
    _currentOffset += _itemSize;
    return (unsafe::Any *)ret;
}

bool AggItemAllocator::allocTrunk() {
    _currentTrunk = malloc(TRUNK_SIZE * _itemSize);
    if (unlikely(!_currentTrunk)) {
        return false;
    }
    _currentOffset = 0;
    _trunks.push_back(_currentTrunk);
    return true;
}

}

