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

#include <algorithm>
#include <memory>
#include <vector>

#include "autil/StringView.h"
#include "autil/mem_pool/PoolBase.h"
#include "indexlib/index/aggregation/IValueIterator.h"
#include "indexlib/util/slice_array/AlignedSliceArray.h"

namespace indexlib::file_system {
class FileWriter;
}

namespace indexlibv2::index {

class FixedSizeValueBlock : public std::enable_shared_from_this<FixedSizeValueBlock>
{
private:
    using Impl = indexlib::util::SliceArray<char>;

public:
    FixedSizeValueBlock(size_t capacity, size_t fixedLen, autil::mem_pool::PoolBase* pool);
    ~FixedSizeValueBlock();

public:
    std::unique_ptr<IValueIterator> CreateIterator() const;

    void Append(const autil::StringView& data)
    {
        assert(data.size() == ValueSize());
        size_t index = _data->GetDataSize() / _data->GetSliceLen();
        size_t offset = _data->GetDataSize() % _data->GetSliceLen();
        char* slice = _data->GetSlice(index);
        memcpy(slice + offset, data.data(), data.size());
        _data->SetMaxValidIndex(_data->GetMaxValidIndex() + data.size());
    }

    size_t Size() const { return _data->GetDataSize() / ValueSize(); }
    size_t Capacity() const { return _capacity; }
    size_t ValueSize() const { return _valueSize; }
    size_t AllocatedBytes() const { return Capacity() * ValueSize(); }
    bool IsFull() const { return Size() >= Capacity(); }
    autil::StringView Get(size_t idx) const
    {
        size_t pos = idx * ValueSize();
        return autil::StringView(_data->Get(pos), ValueSize());
    }
    autil::StringView operator[](size_t idx) const { return Get(idx); }
    size_t GetTotalBytes() const { return Size() * ValueSize(); }

    Status Dump(const std::shared_ptr<indexlib::file_system::FileWriter>& file) const;

private:
    std::unique_ptr<Impl> _data;
    const size_t _capacity;
    const size_t _valueSize;
};

} // namespace indexlibv2::index
