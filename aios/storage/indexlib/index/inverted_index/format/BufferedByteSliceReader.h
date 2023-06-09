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

#include "indexlib/index/inverted_index/format/BufferedByteSlice.h"

namespace indexlib::index {

class BufferedByteSliceReader
{
public:
    BufferedByteSliceReader();
    ~BufferedByteSliceReader() = default;

    void Open(const BufferedByteSlice* bufferedByteSlice);

    /*
     * pos <= ByteSliceSize
     */
    void Seek(uint32_t pos)
    {
        _byteSliceReader.Seek(pos);
        _locationCursor = 0;
        _shortBufferCursor = 0;
    }

    uint32_t Tell() const { return _byteSliceReader.Tell(); }

    // TODO: now user should confirm buffer actual count
    template <typename T>
    bool Decode(T* buffer, size_t count, size_t& decodeCount);

private:
    AtomicValue* CurrentValue();
    void IncValueCursor();

    bool IsValidShortBuffer() const
    {
        uint32_t bufferSize = _bufferedByteSlice->GetBufferSize();
        return bufferSize > 0 && _shortBufferCursor < _multiValue->GetAtomicValueSize() &&
               _bufferedByteSlice->IsShortBufferValid();
    }

private:
    uint8_t _locationCursor;
    uint8_t _shortBufferCursor;
    file_system::ByteSliceReader _byteSliceReader;
    const BufferedByteSlice* _bufferedByteSlice;
    const MultiValue* _multiValue;

    friend class BufferedByteSliceReaderTest;

    AUTIL_LOG_DECLARE();
};

template <typename T>
bool BufferedByteSliceReader::Decode(T* buffer, size_t count, size_t& decodeCount)
{
    // TODO:
    if (count == 0)
        return false;

    FlushInfo flushInfo = _bufferedByteSlice->GetFlushInfo();
    uint32_t byteSliceSize = flushInfo.GetFlushLength();

    if (_byteSliceReader.Tell() >= byteSliceSize && !IsValidShortBuffer()) {
        return false;
    }

    AtomicValue* currentValue = CurrentValue();
    assert(ValueTypeTraits<T>::TYPE == currentValue->GetType());

    if (_byteSliceReader.Tell() >= byteSliceSize) {
        size_t bufferSize = _bufferedByteSlice->GetBufferSize();
        assert(bufferSize <= count);

        const ShortBuffer& shortBuffer = _bufferedByteSlice->GetBuffer();
        const T* src = shortBuffer.GetRowTyped<T>(currentValue->GetLocation());

        memcpy((void*)buffer, (const void*)src, bufferSize * sizeof(T));

        decodeCount = bufferSize;
        _shortBufferCursor++;
    } else {
        decodeCount =
            currentValue->Decode(flushInfo.GetCompressMode(), (uint8_t*)buffer, count * sizeof(T), _byteSliceReader);
    }

    IncValueCursor();

    return true;
}

inline AtomicValue* BufferedByteSliceReader::CurrentValue()
{
    const AtomicValueVector& values = _multiValue->GetAtomicValues();
    assert(_locationCursor < _multiValue->GetAtomicValueSize());
    return values[_locationCursor];
}

inline void BufferedByteSliceReader::IncValueCursor()
{
    _locationCursor++;
    if (_locationCursor == _multiValue->GetAtomicValueSize()) {
        _locationCursor = 0;
    }
}

} // namespace indexlib::index
