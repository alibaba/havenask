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
#include "indexlib/util/slice_array/DefragSliceArray.h"

using namespace std;

using namespace indexlib::util;
namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, DefragSliceArray);

DefragSliceArray::SliceHeader::SliceHeader() : wastedSize(0) {}

DefragSliceArray::DefragSliceArray(const SliceArrayPtr& sliceArray) : _sliceArray(sliceArray) { assert(_sliceArray); }

DefragSliceArray::~DefragSliceArray() {}

uint64_t DefragSliceArray::Append(const void* value, size_t size)
{
    assert(value);

    if (IsOverLength(size, GetSliceLen())) {
        INDEXLIB_FATAL_ERROR(OutOfRange,
                             "data [%ld] can not store in one slice [%ld],"
                             " slice header size [%ld]",
                             size, GetSliceLen(), GetHeaderSize());
    }

    if (!_sliceArray->IsCurrentSliceEnough(size)) {
        int64_t lastSliceIdx = _sliceArray->GetCurrentSliceIdx();
        AllocateNewSlice();
        FreeLastSliceSpace(lastSliceIdx);
    }

    return _sliceArray->Insert(value, size);
}

void DefragSliceArray::AllocateNewSlice()
{
    _sliceArray->AllocateSlice();
    SliceHeader header;
    _sliceArray->Insert(&header, sizeof(header));
}

void DefragSliceArray::FreeLastSliceSpace(int64_t sliceIdx)
{
    if (sliceIdx < 0) {
        return;
    }

    uint64_t curSliceUsedBytes = _sliceArray->GetSliceUsedBytes(sliceIdx);
    int64_t freeSize = GetSliceLen() - curSliceUsedBytes;
    assert(freeSize >= 0);
    FreeBySliceIdx(sliceIdx, freeSize);
}

void DefragSliceArray::Free(uint64_t offset, size_t size)
{
    FreeBySliceIdx(_sliceArray->OffsetToSliceIdx(offset), size);
}

void DefragSliceArray::FreeBySliceIdx(int64_t sliceIdx, size_t size)
{
    IncreaseWastedSize(sliceIdx, size);
    DoFree(size);

    if (NeedDefrag(sliceIdx)) {
        Defrag(sliceIdx);
    }
}
}} // namespace indexlib::util
