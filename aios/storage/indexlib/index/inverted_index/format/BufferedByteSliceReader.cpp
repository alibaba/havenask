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
#include "indexlib/index/inverted_index/format/BufferedByteSliceReader.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, BufferedByteSliceReader);

BufferedByteSliceReader::BufferedByteSliceReader()
{
    _locationCursor = 0;
    _shortBufferCursor = 0;
    _bufferedByteSlice = nullptr;
    _multiValue = nullptr;
}

void BufferedByteSliceReader::Open(const BufferedByteSlice* bufferedByteSlice)
{
    _locationCursor = 0;
    _shortBufferCursor = 0;

    _byteSliceReader.Open(const_cast<util::ByteSliceList*>(bufferedByteSlice->GetByteSliceList())).GetOrThrow();
    _bufferedByteSlice = bufferedByteSlice;
    _multiValue = bufferedByteSlice->GetMultiValue();
}

} // namespace indexlib::index
