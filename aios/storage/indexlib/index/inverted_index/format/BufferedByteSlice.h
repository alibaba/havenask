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

#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/RecyclePool.h"
#include "indexlib/file_system/ByteSliceReader.h"
#include "indexlib/file_system/ByteSliceWriter.h"
#include "indexlib/index/common/AtomicValue.h"
#include "indexlib/index/common/MultiValue.h"
#include "indexlib/index/common/ShortBuffer.h"
#include "indexlib/index/inverted_index/Constant.h"
#include "indexlib/index/inverted_index/format/FlushInfo.h"

namespace indexlib::index {

class BufferedByteSlice
{
public:
    BufferedByteSlice(autil::mem_pool::Pool* byteSlicePool, autil::mem_pool::Pool* bufferPool);
    virtual ~BufferedByteSlice() = default;

    bool Init(const MultiValue* value);

    template <typename T>
    void PushBack(const AtomicValueTyped<T>* value) __ALWAYS_INLINE;

    template <typename T>
    void PushBack(uint8_t row, T value) __ALWAYS_INLINE;

    void EndPushBack()
    {
        _flushInfo.SetIsValidShortBuffer(true);
        MEMORY_BARRIER();
        _buffer.EndPushBack();
    }

    bool NeedFlush(uint8_t needFlushCount = MAX_DOC_PER_RECORD) const { return _buffer.Size() == needFlushCount; }

    // TODO: only one compress_mode
    virtual size_t Flush(uint8_t compressMode); // virtual for test

    virtual void Dump(const std::shared_ptr<file_system::FileWriter>& file)
    {
        _postingListWriter.Dump(file).GetOrThrow();
    }

    virtual size_t EstimateDumpSize() const { return _postingListWriter.GetSize(); }

    size_t GetBufferSize() const { return _buffer.Size(); }

    const ShortBuffer& GetBuffer() const { return _buffer; }

    // TODO: now we only support 512M count
    // should be used in realtime build
    size_t GetTotalCount() const { return _flushInfo.GetFlushCount() + _buffer.Size(); }

    FlushInfo GetFlushInfo() const { return _flushInfo; }

    const MultiValue* GetMultiValue() const { return _buffer.GetMultiValue(); }

    const util::ByteSliceList* GetByteSliceList() const { return _postingListWriter.GetByteSliceList(); }

    // SnapShot buffer is readonly
    void SnapShot(BufferedByteSlice* buffer) const;

    bool IsShortBufferValid() const { return _flushInfo.IsValidShortBuffer(); }

    autil::mem_pool::Pool* GetBufferPool() const { return _buffer.GetPool(); }

    util::ByteSliceList* GetByteSliceList() { return _postingListWriter.GetByteSliceList(); }

protected:
    // template function
    virtual size_t DoFlush(uint8_t compressMode);

    FlushInfo _flushInfo;
    ShortBuffer _buffer;
    file_system::ByteSliceWriter _postingListWriter;

private:
    void SnapShotByteSlice(BufferedByteSlice* buffer) const;

    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////

template <typename T>
inline void BufferedByteSlice::PushBack(const AtomicValueTyped<T>* value)
{
    assert(value);
    _buffer.PushBack(value->GetLocation(), value->GetValue());
}

template <typename T>
inline void BufferedByteSlice::PushBack(uint8_t row, T value)
{
    _buffer.PushBack(row, value);
}
} // namespace indexlib::index
