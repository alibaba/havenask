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
#include "indexlib/util/slice_array/ByteAlignedSliceArray.h"

using namespace std;

using namespace indexlib::util;
namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, ByteAlignedSliceArray);

ByteAlignedSliceArray::ByteAlignedSliceArray(uint32_t sliceLen, uint32_t initSliceNum, autil::mem_pool::PoolBase* pool,
                                             bool isOwner)
    : AlignedSliceArray<char>(sliceLen, initSliceNum, pool, isOwner)
{
}

ByteAlignedSliceArray::ByteAlignedSliceArray(const ByteAlignedSliceArray& src) : AlignedSliceArray<char>(src) {}

ByteAlignedSliceArray::~ByteAlignedSliceArray() {}
}} // namespace indexlib::util
