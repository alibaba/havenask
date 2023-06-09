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
#include "autil/StringView.h"
#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/index/kkv/common/KKVDocs.h"

namespace indexlibv2::index {

class KKVResultBuffer
{
public:
    KKVResultBuffer(size_t bufferSize, uint32_t skeyCountLimit, autil::mem_pool::Pool* pool)
        : _bufferSize(bufferSize)
        , _cursor(0)
        , _buffer(pool)
        , _resultLimit(skeyCountLimit)
    {
        assert(pool);
    }

public:
    // for read
    uint64_t GetCurrentSkey() const { return _buffer[_cursor].skey; }
    void GetCurrentValue(autil::StringView& value) const
    {
        assert(!_buffer[_cursor].skeyDeleted);
        value = _buffer[_cursor].value;
    }
    uint64_t GetCurrentTimestamp() const { return _buffer[_cursor].timestamp; }
    bool IsCurrentDocInCache() const { return _buffer[_cursor].inCache; }
    bool IsValid() const { return _cursor < _buffer.size(); }
    void MoveToNext()
    {
        while (true) {
            _cursor++;
            if (!_buffer[_cursor].duplicatedKey) {
                break;
            }
            if (!IsValid()) {
                break;
            }
        }
    }

public:
    // for write
    bool IsFull() const { return _buffer.size() >= _bufferSize; }
    bool ReachLimit() { return _resultLimit <= 0; }
    size_t Size() const noexcept { return _buffer.size(); }
    void Clear()
    {
        _buffer.clear();
        _cursor = 0;
    }
    void Swap(KKVDocs& docs) { _buffer.swap(docs); }
    void EmplaceBack(KKVDoc&& doc)
    {
        _resultLimit--;
        _buffer.emplace_back(std::move(doc));
    }
    KKVDoc& operator[](size_t idx) { return _buffer[idx]; }
    const KKVDoc& operator[](size_t idx) const { return _buffer[idx]; }

    KKVDoc& Back() { return _buffer.back(); }
    const KKVDoc& Back() const { return _buffer.back(); }

private:
    size_t _bufferSize;
    size_t _cursor;
    KKVDocs _buffer;
    uint32_t _resultLimit;
};

} // namespace indexlibv2::index
