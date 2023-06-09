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
#include "indexlib/index/inverted_index/format/BufferedByteSlice.h"

#include "autil/mem_pool/Pool.h"

namespace indexlib::index {

AUTIL_LOG_SETUP(indexlib.index, BufferedByteSlice);

BufferedByteSlice::BufferedByteSlice(autil::mem_pool::Pool* byteSlicePool, autil::mem_pool::Pool* bufferPool)
    : _buffer(bufferPool)
    , _postingListWriter(byteSlicePool)
{
}

bool BufferedByteSlice::Init(const MultiValue* value)
{
    _buffer.Init(value);
    return true;
}

size_t BufferedByteSlice::Flush(uint8_t compressMode)
{
    // TODO: used for async dump
    // bufferPool will be released in InMemorySegmentWriter,
    // _buffer is unreadable at this time
    if (_buffer.Size() == 0) {
        return 0;
    }

    size_t flushSize = DoFlush(compressMode);
    MEMORY_BARRIER();

    FlushInfo tempFlushInfo;
    tempFlushInfo.SetFlushCount(_flushInfo.GetFlushCount() + _buffer.Size());
    tempFlushInfo.SetFlushLength(_flushInfo.GetFlushLength() + flushSize);
    tempFlushInfo.SetCompressMode(compressMode);
    tempFlushInfo.SetIsValidShortBuffer(false);

    MEMORY_BARRIER();
    _flushInfo = tempFlushInfo;
    MEMORY_BARRIER();

    _buffer.Clear();
    return flushSize;
}

size_t BufferedByteSlice::DoFlush(uint8_t compressMode)
{
    uint32_t flushSize = 0;
    const AtomicValueVector& atomicValues = GetMultiValue()->GetAtomicValues();

    for (size_t i = 0; i < atomicValues.size(); ++i) {
        AtomicValue* atomicValue = atomicValues[i];
        uint8_t* buffer = _buffer.GetRow(atomicValue->GetLocation());
        flushSize +=
            atomicValue->Encode(compressMode, _postingListWriter, buffer, _buffer.Size() * atomicValue->GetSize());
    }

    return flushSize;
}

void BufferedByteSlice::SnapShot(BufferedByteSlice* buffer) const
{
    buffer->Init(GetMultiValue());

    SnapShotByteSlice(buffer);
    _buffer.SnapShot(buffer->_buffer);

    if (_flushInfo.GetFlushLength() > buffer->_flushInfo.GetFlushLength()) {
        buffer->_buffer.Clear();
        SnapShotByteSlice(buffer);
    }
}

void BufferedByteSlice::SnapShotByteSlice(BufferedByteSlice* buffer) const
{
    buffer->_flushInfo = _flushInfo;
    _postingListWriter.SnapShot(buffer->_postingListWriter);
}

} // namespace indexlib::index
