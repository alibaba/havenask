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

#include "indexlib/util/byte_slice_list/ByteSliceList.h"

namespace indexlib { namespace util {
class ByteSliceListIterator
{
public:
    ByteSliceListIterator(const ByteSliceList* sliceList);
    ByteSliceListIterator(const ByteSliceListIterator& other);
    ~ByteSliceListIterator() {}

public:
    // [beginPos, endPos)
    bool SeekSlice(size_t beginPos);
    bool HasNext(size_t endPos);
    void Next(void*& data, size_t& size);

private:
    const ByteSliceList* _sliceList;
    ByteSlice* _slice;
    size_t _posInSlice;
    size_t _seekedSliceSize;
    size_t _endPos;

private:
    friend class ByteSliceListIteratorTest;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ByteSliceListIterator> ByteSliceListIteratorPtr;
}} // namespace indexlib::util
