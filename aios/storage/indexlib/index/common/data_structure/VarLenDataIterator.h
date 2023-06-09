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

#include "indexlib/index/common/RadixTree.h"
#include "indexlib/index/common/TypedSliceList.h"

namespace indexlibv2::index {

class VarLenDataIterator
{
    const static uint64_t INVALID_CURSOR = (uint64_t)-1;

public:
    VarLenDataIterator(indexlib::index::RadixTree* data, indexlib::index::TypedSliceList<uint64_t>* offsets)
        : _data(data)
        , _offsets(offsets)
        , _currentData(NULL)
        , _dataLength(0)
        , _currentOffset(0)
        , _cursor(INVALID_CURSOR)
    {
    }

    ~VarLenDataIterator()
    {
        _data = NULL;
        _offsets = NULL;
    }

public:
    bool HasNext();
    void Next();
    void GetCurrentData(uint64_t& dataLength, uint8_t*& data);
    void GetCurrentOffset(uint64_t& offset);

private:
    indexlib::index::RadixTree* _data;
    indexlib::index::TypedSliceList<uint64_t>* _offsets;
    uint8_t* _currentData;
    uint64_t _dataLength;
    uint64_t _currentOffset;
    uint64_t _cursor;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
