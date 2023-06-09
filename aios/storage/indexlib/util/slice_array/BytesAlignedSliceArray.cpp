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
#include "indexlib/util/slice_array/BytesAlignedSliceArray.h"

#include "indexlib/util/MmapPool.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, BytesAlignedSliceArray);

BytesAlignedSliceArray::BytesAlignedSliceArray(const SimpleMemoryQuotaControllerPtr& memController, uint64_t sliceLen,
                                               uint32_t maxSliceNum)
    : _sliceArray(sliceLen, maxSliceNum, new MmapPool, true)
    , _maxSliceNum(maxSliceNum)
    , _usedBytes(0)
    , _curSliceIdx(-1)
    , _memController(memController)
{
    _curCursor = _sliceArray.GetSliceLen() + 1;
}

BytesAlignedSliceArray::~BytesAlignedSliceArray() {}
}} // namespace indexlib::util
