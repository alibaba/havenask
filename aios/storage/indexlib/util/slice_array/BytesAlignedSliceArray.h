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
#include <memory>

#include "indexlib/util/MMapAllocator.h"
#include "indexlib/util/memory_control/SimpleMemoryQuotaController.h"
#include "indexlib/util/slice_array/AlignedSliceArray.h"

namespace indexlib { namespace util {

class SliceMemoryMetrics
{
public:
    SliceMemoryMetrics() {}
    virtual ~SliceMemoryMetrics() {}

public:
    virtual void Increase(size_t size) {}
    virtual void Decrease(size_t size) {}
};
typedef std::shared_ptr<SliceMemoryMetrics> SliceMemoryMetricsPtr;

//////////////////////////////////////////////////////////////////////
class BytesAlignedSliceArray
{
public:
    typedef std::vector<int64_t> SliceIdxVector;
    typedef AlignedSliceArray<char> SliceArray;

public:
    BytesAlignedSliceArray(const SimpleMemoryQuotaControllerPtr& memController, uint64_t sliceLen,
                           uint32_t maxSliceNum = 1);
    ~BytesAlignedSliceArray();

public:
    int64_t Insert(const void* value, size_t size) noexcept(false);
    uint64_t GetTotalUsedBytes() const noexcept { return _usedBytes; }
    void* GetValueAddress(int64_t offset) noexcept;

public:
    int64_t OffsetToSliceIdx(int64_t offset) const { return _sliceArray.OffsetToSliceIdx(offset); }
    int64_t SliceIdxToOffset(int64_t sliceIdx) const { return _sliceArray.SliceIdxToOffset(sliceIdx); }

    int64_t GetSliceNum() const { return _sliceArray.GetSliceNum(); }
    uint64_t GetSliceLen() const { return _sliceArray.GetSliceLen(); }
    uint64_t GetCurrentOffset() const { return SliceIdxToOffset(_curSliceIdx) + _curCursor; }

    uint64_t GetSliceUsedBytes(int64_t sliceIdx) const
    {
        if (sliceIdx < 0 || sliceIdx >= (int64_t)_sliceUsedSizeVector.size()) {
            return 0;
        }
        return _sliceUsedSizeVector[sliceIdx];
    }
    int64_t GetCurrentSliceIdx() const { return _curSliceIdx; }
    SliceIdxVector GetValidSliceIdxs() const;
    bool IsCurrentSliceEnough(size_t size);

    void AllocateSlice() noexcept(false);
    void ReleaseSlice(int64_t sliceIdx);

    void SetSliceMemoryMetrics(const std::shared_ptr<SliceMemoryMetrics>& storageMetrics) noexcept
    {
        _storageMetrics = storageMetrics;
    }

private:
    int64_t FindValidSliceIdx();
    BytesAlignedSliceArray(const BytesAlignedSliceArray& src) : _sliceArray(src._sliceArray) {}

private:
    SliceArray _sliceArray;
    std::shared_ptr<SliceMemoryMetrics> _storageMetrics;
    std::atomic<size_t> _usedBytes = 0;
    int32_t _maxSliceNum;
    uint64_t _curCursor;
    int64_t _curSliceIdx = -1;
    std::vector<uint64_t> _sliceUsedSizeVector;
    SimpleMemoryQuotaControllerPtr _memController;

private:
    AUTIL_LOG_DECLARE();
    friend class BytesAlignedSliceArrayTest;
};

typedef std::shared_ptr<BytesAlignedSliceArray> BytesAlignedSliceArrayPtr;

inline bool BytesAlignedSliceArray::IsCurrentSliceEnough(size_t size)
{
    uint64_t sliceLen = GetSliceLen();
    if (_curCursor == sliceLen || _curCursor + size > sliceLen) {
        return false;
    }
    return true;
}

inline int64_t BytesAlignedSliceArray::Insert(const void* value, size_t size) noexcept(false)
{
    assert(size <= GetSliceLen());
    if (!IsCurrentSliceEnough(size)) {
        AllocateSlice();
    }

    if (size == 0) {
        return SliceIdxToOffset(_curSliceIdx) + _curCursor;
    }

    assert(value);
    void* ptr = _sliceArray.GetSliceWithoutAllocate(_curSliceIdx) + _curCursor;
    assert(ptr);
    memcpy(ptr, value, size);
    uint64_t cursor = _curCursor;
    _curCursor += size;
    _usedBytes += size;
    _sliceUsedSizeVector[_curSliceIdx] += size;
    _memController->Allocate(size);
    if (_storageMetrics) {
        _storageMetrics->Increase(size);
    }
    return SliceIdxToOffset(_curSliceIdx) + cursor;
}

inline int64_t BytesAlignedSliceArray::FindValidSliceIdx()
{
    int64_t sliceIdx = 0;
    for (; sliceIdx < (int64_t)_sliceArray.GetSliceNum(); sliceIdx++) {
        if (_sliceArray.GetSliceWithoutAllocate(sliceIdx) == NULL) {
            return sliceIdx;
        }
    }
    return sliceIdx;
}

inline void* BytesAlignedSliceArray::GetValueAddress(int64_t offset) noexcept
{
    return _sliceArray.OffsetToAddress(offset);
}

inline void BytesAlignedSliceArray::AllocateSlice() noexcept(false)
{
    if (_curSliceIdx + 1 >= _maxSliceNum) {
        INDEXLIB_FATAL_ERROR(OutOfRange,
                             "Slice index out of range,"
                             "curIndex[%ld] + 1 >= reservedSliceNum[%u]",
                             _curSliceIdx, _maxSliceNum);
    }

    _curSliceIdx = FindValidSliceIdx();
    _curCursor = 0;
    _sliceArray.AllocateSlice(_curSliceIdx);

    if (_curSliceIdx >= (int64_t)_sliceUsedSizeVector.size()) {
        _sliceUsedSizeVector.resize(_curSliceIdx + 1);
    }
    _sliceUsedSizeVector[_curSliceIdx] = 0;
}

inline void BytesAlignedSliceArray::ReleaseSlice(int64_t sliceIdx)
{
    if (sliceIdx < 0 || sliceIdx >= _sliceArray.GetSliceNum()) {
        INDEXLIB_FATAL_ERROR(OutOfRange,
                             "Release slice failed,"
                             "sliceIdx [%ld], sliceNum [%u]",
                             _curSliceIdx, _sliceArray.GetSliceNum());
    }

    _sliceArray.ReleaseSlice(sliceIdx);

    uint64_t sliceUsedBytes = GetSliceUsedBytes(sliceIdx);
    _usedBytes -= sliceUsedBytes;
    _memController->Free(sliceUsedBytes);
    if (_storageMetrics) {
        _storageMetrics->Decrease(sliceUsedBytes);
    }
    _sliceUsedSizeVector[sliceIdx] = 0;
    if (_curSliceIdx == sliceIdx) {
        _curSliceIdx = -1;
        _curCursor = _sliceArray.GetSliceLen() + 1;
    }
}

inline BytesAlignedSliceArray::SliceIdxVector BytesAlignedSliceArray::GetValidSliceIdxs() const
{
    SliceIdxVector vec;
    for (uint32_t sliceIdx = 0; sliceIdx < _sliceArray.GetSliceNum(); sliceIdx++) {
        const void* slice = _sliceArray.GetSlice(sliceIdx);
        if (slice != NULL) {
            vec.push_back(sliceIdx);
        }
    }
    return vec;
}
}} // namespace indexlib::util
