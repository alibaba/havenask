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
#include "indexlib/index/inverted_index/format/skiplist/SkipListReader.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, SkipListReader);

SkipListReader::SkipListReader() : _start(0), _end(0), _skippedItemCount(-1) {}

SkipListReader::SkipListReader(const SkipListReader& other)
    : _start(other._start)
    , _end(other._end)
    , _byteSliceReader(other._byteSliceReader)
    , _skippedItemCount(other._skippedItemCount)

{
}

SkipListReader::~SkipListReader() {}

void SkipListReader::Load(const util::ByteSliceList* byteSliceList, uint32_t start, uint32_t end)
{
    // TODO: throw exception
    assert(start <= byteSliceList->GetTotalSize());
    assert(end <= byteSliceList->GetTotalSize());
    assert(start <= end);
    _start = start;
    _end = end;
    _byteSliceReader.Open(const_cast<util::ByteSliceList*>(byteSliceList));
    _byteSliceReader.Seek(start);
}

void SkipListReader::Load(util::ByteSlice* byteSlice, uint32_t start, uint32_t end)
{
    // TODO: throw exception
    assert(start <= byteSlice->size);
    assert(end <= byteSlice->size);
    assert(start <= end);
    _start = start;
    _end = end;
    _byteSliceReader.Open(byteSlice);
    _byteSliceReader.Seek(start);
}
} // namespace indexlib::index
