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

#include "indexlib/util/slice_array/AlignedSliceArray.h"

namespace indexlib { namespace util {
class ByteAlignedSliceArray : public AlignedSliceArray<char>
{
public:
    ByteAlignedSliceArray(uint32_t sliceLen, uint32_t initSliceNum = 1, autil::mem_pool::PoolBase* pool = NULL,
                          bool isOwner = false);
    ByteAlignedSliceArray(const ByteAlignedSliceArray& src);
    ~ByteAlignedSliceArray();

public:
    template <typename T>
    inline void SetTypedValue(int64_t index, const T& value);

    template <typename T>
    inline void GetTypedValue(int64_t index, T& value);

    inline void SetByteList(int64_t index, char value, int64_t size);

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ByteAlignedSliceArray> ByteAlignedSliceArrayPtr;

////////////////////////////////////////////////////////////////////////
template <typename T>
inline void ByteAlignedSliceArray::SetTypedValue(int64_t index, const T& value)
{
    SetList(index, (char*)&value, sizeof(T));
}

template <typename T>
inline void ByteAlignedSliceArray::GetTypedValue(int64_t index, T& value)
{
    Read(index, (char*)&value, sizeof(T));
}

inline void ByteAlignedSliceArray::SetByteList(int64_t index, char value, int64_t size)
{
    uint32_t sliceIndex = index >> _powerOf2;
    uint32_t offset = index & _powerMark;

    uint32_t sliceRemain = this->_sliceLen - offset;
    uint32_t toSetNum = 0;
    int64_t remainSize = size;

    while (remainSize > 0) {
        value += toSetNum;
        char* slice = this->GetSlice(sliceIndex++);
        if (remainSize <= sliceRemain) {
            toSetNum = remainSize;
        } else {
            toSetNum = sliceRemain;
            sliceRemain = this->_sliceLen;
        }

        memset(slice + offset, value, toSetNum);
        remainSize -= toSetNum;
        offset = 0;
    }

    if ((index + size) > this->_maxValidIndex) {
        this->_maxValidIndex = index + size - 1;
    }
}
}} // namespace indexlib::util
