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

#include "indexlib/util/slice_array/BytesAlignedSliceArray.h"

namespace indexlib { namespace util {

class DefragSliceArray
{
public:
    struct SliceHeader {
        SliceHeader();
        int32_t wastedSize;
    };

    typedef BytesAlignedSliceArray SliceArray;
    typedef std::shared_ptr<BytesAlignedSliceArray> SliceArrayPtr;

public:
    DefragSliceArray(const SliceArrayPtr& sliceArray);
    virtual ~DefragSliceArray();

public:
    uint64_t Append(const void* value, size_t size);
    void Free(uint64_t offset, size_t size);
    const void* Get(uint64_t offset) const { return _sliceArray->GetValueAddress(offset); }

    static bool IsOverLength(size_t length, size_t sliceLen) { return (length + sizeof(SliceHeader)) > sliceLen; }

protected:
    // wrapper for SliceArray
    int64_t GetCurrentSliceIdx() const { return _sliceArray->GetCurrentSliceIdx(); }
    int64_t SliceIdxToOffset(int64_t sliceIdx) const { return _sliceArray->SliceIdxToOffset(sliceIdx); }
    uint64_t GetSliceLen() const { return _sliceArray->GetSliceLen(); }
    void ReleaseSlice(int64_t sliceIdx) { _sliceArray->ReleaseSlice(sliceIdx); }
    // wrapper for Header
    size_t GetHeaderSize() const { return sizeof(SliceHeader); }
    int32_t GetWastedSize(int64_t sliceIdx) const;
    void SetWastedSize(int64_t sliceIdx, int32_t wastedSize)
    {
        ((SliceHeader*)GetSlice(sliceIdx))->wastedSize = wastedSize;
    }

    // for test
    void IncreaseWastedSize(int64_t sliceIdx, int32_t wastedSize)
    {
        ((SliceHeader*)GetSlice(sliceIdx))->wastedSize += wastedSize;
    }

    int64_t GetSliceNum() const { return _sliceArray->GetSliceNum(); }

private:
    virtual void Defrag(int64_t sliceIdx) {}
    virtual void DoFree(size_t size) {}
    virtual bool NeedDefrag(int64_t sliceIdx) { return false; }
    virtual void AllocateNewSlice();

private:
    void FreeLastSliceSpace(int64_t sliceIdx);
    void* GetSlice(int64_t sliceIdx) const;
    void FreeBySliceIdx(int64_t sliceIdx, size_t size);

protected:
    SliceArrayPtr _sliceArray;

private:
    friend class DefragSliceArrayTest;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<DefragSliceArray> DefragSliceArrayPtr;

////////////////////////////////////////////////////
inline void* DefragSliceArray::GetSlice(int64_t sliceIdx) const
{
    int64_t sliceOffset = _sliceArray->SliceIdxToOffset(sliceIdx);
    return _sliceArray->GetValueAddress(sliceOffset);
}

inline int32_t DefragSliceArray::GetWastedSize(int64_t sliceIdx) const
{
    SliceHeader* header = (SliceHeader*)GetSlice(sliceIdx);
    if (header) {
        return header->wastedSize;
    }
    return 0;
}
}} // namespace indexlib::util
