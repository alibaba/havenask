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

#include "indexlib/common_define.h"
#include "indexlib/index/common/RadixTree.h"
#include "indexlib/index/common/TypedSliceList.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class VarLenDataIterator
{
    const static uint64_t INVALID_CURSOR = (uint64_t)-1;

public:
    VarLenDataIterator(RadixTree* data, TypedSliceList<uint64_t>* offsets)
        : mData(data)
        , mOffsets(offsets)
        , mCurrentData(NULL)
        , mDataLength(0)
        , mCurrentOffset(0)
        , mCursor(INVALID_CURSOR)
    {
    }

    ~VarLenDataIterator()
    {
        mData = NULL;
        mOffsets = NULL;
    }

public:
    bool HasNext();
    void Next();
    void GetCurrentData(uint64_t& dataLength, uint8_t*& data);
    void GetCurrentOffset(uint64_t& offset);

private:
    RadixTree* mData;
    TypedSliceList<uint64_t>* mOffsets;
    uint8_t* mCurrentData;
    uint64_t mDataLength;
    uint64_t mCurrentOffset;
    uint64_t mCursor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VarLenDataIterator);
}} // namespace indexlib::index
