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

#include "autil/CommonMacros.h"
#include "cava/common/Common.h"
#include "cava/runtime/Allocator.h"

#include "ha3/cava/AggItemAllocator.h"

class CavaCtx;

using namespace std;

namespace unsafe {

AggItemAllocator *AggItemAllocator::create(CavaCtx *ctx, uint itemSize) {
    if (unlikely(!itemSize)) {
        return NULL;
    }
    uint alignItemSize = (itemSize + DEFAULT_ALIGN_SIZE - 1) & ~(DEFAULT_ALIGN_SIZE - 1);
    return cava::AllocUtil::createNativeObject<AggItemAllocator>(ctx, alignItemSize);
}

}
