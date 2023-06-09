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

#include "autil/mem_pool/PoolBase.h"
#include "indexlib/index/aggregation/IKeyIterator.h"
#include "indexlib/util/slice_array/AlignedSliceArray.h"

namespace indexlibv2::index {

class KeyVector
{
private:
    using Impl = indexlib::util::AlignedSliceArray<uint64_t>;

public:
    class Iterator final : public IKeyIterator
    {
    public:
        Iterator(const Impl* data, size_t count) : _data(data), _count(count), _cursor(0) {}

    public:
        bool HasNext() const override { return _cursor < _count; }
        uint64_t Next() override { return (*_data)[_cursor++]; }

    private:
        const Impl* _data;
        size_t _count;
        size_t _cursor;
    };

public:
    explicit KeyVector(autil::mem_pool::PoolBase* pool, size_t sliceLen = DEFAULT_SLICE_LEN);
    ~KeyVector();

public:
    void Append(uint64_t key);
    size_t GetKeyCount() const;
    std::unique_ptr<IKeyIterator> CreateIterator() const
    {
        return std::make_unique<Iterator>(_data.get(), _data->GetDataSize());
    }

private:
    std::shared_ptr<Impl> _data;

private:
    static constexpr size_t DEFAULT_SLICE_LEN = 4096 / sizeof(uint64_t); // use 1 page memory
};

} // namespace indexlibv2::index
