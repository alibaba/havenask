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

#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Status.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/util/ValueWriter.h"
#include "indexlib/util/slice_array/SliceArray.h"

namespace indexlibv2::index {

template <typename ValueOffsetType>
class ExpandableValueAccessor
{
public:
    using SingleValueAccessor = indexlib::util::ValueWriter<ValueOffsetType>;

public:
    ExpandableValueAccessor(autil::mem_pool::PoolBase* pool, bool enableRewrite)
        : _pool(pool)
        , _enableRewrite(enableRewrite)
    {
        assert(pool);
    }

    ExpandableValueAccessor(bool enableRewrite) : _pool(nullptr), _enableRewrite(enableRewrite) {}

    ~ExpandableValueAccessor() = default;

public:
    Status Init(ValueOffsetType sliceLen, size_t sliceNum);

    const char* GetValue(ValueOffsetType valueOffset) const
    {
        ValueOffsetType idx = valueOffset / _sliceLen;
        assert(_valueAccessors.size() > idx);
        ValueOffsetType localValueOffset = valueOffset - _sliceLen * idx;
        return _valueAccessors[idx].GetValue(localValueOffset);
    }

    Status Append(const autil::StringView& value, ValueOffsetType& valueOffset);

    Status Rewrite(const autil::StringView& value, ValueOffsetType valueOffset)
    {
        if (!_enableRewrite) {
            return Status::InternalError("value rewrite is disabled");
        }
        ValueOffsetType idx = valueOffset / _sliceLen;
        assert(_valueAccessors.size() > idx);
        ValueOffsetType localValueOffset = valueOffset - _sliceLen * idx;
        _valueAccessors[idx].Rewrite(value, localValueOffset);
        return Status::OK();
    }

    indexlib::file_system::FSResult<size_t>
    Dump(const std::shared_ptr<indexlib::file_system::FileWriter>& fileWriter) const
    {
        size_t size = 0ul;
        for (int32_t idx = 0; idx <= _curAccessorIdx; ++idx) {
            const auto& accessor = _valueAccessors[idx];
            auto result = fileWriter->Write(accessor.GetBaseAddress(), accessor.GetUsedBytes());
            if (!result.OK()) {
                return result;
            }
            size += result.Value();
        }
        return indexlib::file_system::FSResult(indexlib::file_system::FSEC_OK, size);
    }

    ValueOffsetType GetUsedBytes() const { return _usedBytes; }

    ValueOffsetType GetReserveBytes() const { return _reserveBytes; }

    ValueOffsetType GetExpandMemoryInBytes() const
    {
        return _sliceAllocator->GetSliceLen() * _sliceAllocator->GetSliceNum();
    }

private:
    Status Expand();

private:
    ValueOffsetType _reserveBytes {0};
    ValueOffsetType _usedBytes {0};
    size_t _sliceNum {0};
    ValueOffsetType _sliceLen {0};
    ValueOffsetType _baseOffset {0};
    std::unique_ptr<indexlib::util::SliceArray<char>> _sliceAllocator;
    std::vector<SingleValueAccessor> _valueAccessors;
    int32_t _curAccessorIdx {-1};
    autil::mem_pool::PoolBase* _pool {nullptr};
    bool _enableRewrite {false};

private:
    AUTIL_LOG_DECLARE();
    friend class ExpandableValueWriterTest;
};
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, ExpandableValueAccessor, ValueOffsetType);

template <typename ValueOffsetType>
Status ExpandableValueAccessor<ValueOffsetType>::Init(ValueOffsetType sliceLen, size_t sliceNum)
{
    if (sliceLen <= 0 || sliceNum == 0) {
        return Status::InvalidArgs("both slice len [%ld] and slice num [%lu] must be positive value", (int64_t)sliceLen,
                                   sliceNum);
    }
    if (__builtin_mul_overflow(sliceLen, sliceNum, &_reserveBytes)) {
        return Status::InvalidArgs("overflow when multiply slice len [%lu] by slice num [%lu]", (size_t)sliceLen,
                                   sliceNum);
    }
    _sliceLen = sliceLen;
    _sliceNum = sliceNum;
    bool ownPool = false;
    if (_pool == nullptr) {
        _pool = new autil::mem_pool::UnsafePool(1024 * 1024);
        ownPool = true;
    }
    _sliceAllocator =
        std::make_unique<indexlib::util::SliceArray<char>>(sliceLen, /*initSliceNum*/ 1, _pool, /*isOwner*/ ownPool);
    _valueAccessors.assign(sliceNum, SingleValueAccessor(_enableRewrite));

    auto status = Expand();
    if (!status.IsOK()) {
        return status;
    }
    AUTIL_LOG(INFO, "writer init with slice len [%lu], slice num [%lu]", (size_t)sliceLen, sliceNum);
    return Status::OK();
}

template <typename ValueOffsetType>
Status ExpandableValueAccessor<ValueOffsetType>::Append(const autil::StringView& value, ValueOffsetType& valueOffset)
{
    auto writer = &_valueAccessors[_curAccessorIdx];
    ValueOffsetType wastedBytes = 0;

    ValueOffsetType usedBytes = writer->GetUsedBytes();
    ValueOffsetType reservedBytes = writer->GetReserveBytes();
    if (usedBytes + value.size() > reservedBytes) {
        uint64_t sliceLen = _sliceAllocator->GetSliceLen();
        if (value.size() > sliceLen) {
            return Status::OutOfRange("val size [%lu] bytes out of range [0, %lu]", value.size(), sliceLen);
        }
        auto status = Expand();
        if (!status.IsOK()) {
            return status;
        }

        wastedBytes = reservedBytes - usedBytes;
        if (wastedBytes > 0) {
            std::string padding(wastedBytes, '\0');
            writer->Append(autil::StringView(padding.data(), padding.size()));
            assert(writer->GetUsedBytes() == reservedBytes);
        }

        writer = &_valueAccessors[_curAccessorIdx];
    }
    valueOffset = _baseOffset + writer->Append(value);
    _usedBytes += wastedBytes + value.size();

    return Status::OK();
}

template <typename ValueOffsetType>
Status ExpandableValueAccessor<ValueOffsetType>::Expand()
{
    int32_t sliceNum = _sliceAllocator->GetSliceNum();
    uint64_t sliceLen = _sliceAllocator->GetSliceLen();

    if (sliceNum * sliceLen >= _reserveBytes) {
        return Status::NoMem("slice [%d] * [%lu] reaches limit [%lu B]", sliceNum, sliceLen, _reserveBytes);
    }

    char* slice = _sliceAllocator->AllocateSlice(/*sliceIndex*/ _curAccessorIdx + 1);
    if (slice == nullptr) {
        return Status::Unknown("allocate slice len [%lu] failed", sliceLen);
    }
    ++_curAccessorIdx;
    _valueAccessors[_curAccessorIdx].Init(slice, sliceLen);
    _baseOffset = sliceLen * _curAccessorIdx;

    return Status::OK();
}

} // namespace indexlibv2::index
