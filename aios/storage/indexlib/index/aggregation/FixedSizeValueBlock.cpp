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
#include "indexlib/index/aggregation/FixedSizeValueBlock.h"

#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/aggregation/RandomAccessValueIterator.h"

namespace indexlibv2::index {

class BlockIterator final : public RandomAccessValueIterator
{
public:
    BlockIterator(const std::shared_ptr<const FixedSizeValueBlock>& data, size_t size)
        : _data(data)
        , _size(size)
        , _cursor(0)
    {
    }

public:
    bool HasNext() const override { return _cursor < _size; }
    Status Next(autil::mem_pool::PoolBase* /*unused*/, autil::StringView& value) override
    {
        value = Get(_cursor++);
        return Status::OK();
    }
    size_t Size() const override { return _size; }
    autil::StringView Get(size_t idx) const override { return _data->Get(idx); }

private:
    std::shared_ptr<const FixedSizeValueBlock> _data;
    const size_t _size;
    size_t _cursor;
};

FixedSizeValueBlock::FixedSizeValueBlock(size_t capacity, size_t fixedLen, autil::mem_pool::PoolBase* pool)
    : _capacity(capacity)
    , _valueSize(fixedLen)
{
    static constexpr uint32_t ONE_PAGE = 4096;
    uint32_t countPerSlice = ONE_PAGE / _valueSize;
    uint32_t sliceLen = countPerSlice * _valueSize;
    _data = std::make_unique<Impl>(sliceLen, 1, pool);
}

FixedSizeValueBlock::~FixedSizeValueBlock() = default;

std::unique_ptr<IValueIterator> FixedSizeValueBlock::CreateIterator() const
{
    return std::make_unique<BlockIterator>(shared_from_this(), Size());
}

Status FixedSizeValueBlock::Dump(const std::shared_ptr<indexlib::file_system::FileWriter>& file) const
{
    uint32_t sliceCount = _data->GetSliceNum();
    uint32_t i = 0;
    for (; i + 1 < sliceCount; ++i) {
        const char* data = _data->GetSlice(i);
        RETURN_STATUS_DIRECTLY_IF_ERROR(file->Write(data, _data->GetSliceLen()).Status());
    }
    if (i < _data->GetSliceNum()) {
        const char* data = _data->GetSlice(i);
        RETURN_STATUS_DIRECTLY_IF_ERROR(file->Write(data, _data->GetLastSliceDataLen()).Status());
    }
    return Status::OK();
}

} // namespace indexlibv2::index
