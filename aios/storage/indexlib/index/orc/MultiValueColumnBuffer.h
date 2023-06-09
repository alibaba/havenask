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

#include <vector>

#include "autil/Lock.h"
#include "autil/MultiValueType.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeConvertor.h"
#include "indexlib/index/orc/ColumnBuffer.h"
#include "indexlib/index/orc/TypeTraits.h"

namespace indexlibv2::index {

template <typename T>
class MultiValueColumnBuffer final : public ColumnBuffer
{
public:
    using SignedType = typename make_signed_wrapper<T>::type;
    using ColumnVectorType = typename OrcTypeTraits<T>::ColumnVectorType;

    MultiValueColumnBuffer(const config::FieldConfig* fieldConfig, uint64_t batchSize)
        : ColumnBuffer(fieldConfig, batchSize)
    {
    }

    MultiValueColumnBuffer(const MultiValueColumnBuffer&) = delete;
    MultiValueColumnBuffer& operator=(const MultiValueColumnBuffer&) = delete;
    MultiValueColumnBuffer(MultiValueColumnBuffer&&) = delete;
    MultiValueColumnBuffer& operator=(MultiValueColumnBuffer&&) = delete;

    ~MultiValueColumnBuffer() = default;

    Status Init(orc::ColumnVectorBatch* batch) override
    {
        if (_batchSize <= 0) {
            return Status::InvalidArgs("batch size should be larger than 0");
        }
        _parent = dynamic_cast<orc::ListVectorBatch*>(batch);
        if (_parent == nullptr) {
            return Status::InvalidArgs("fieldName[%s] fail to dynamic_cast ColumnVectorBatch to ListVectorBatch",
                                       _field->GetFieldName().c_str());
        }
        _child = dynamic_cast<ColumnVectorType*>(_parent->elements.get());
        if (_child == nullptr) {
            return Status::InvalidArgs("fieldName[%s] fail to dynamic_cast ColumnVectorBatch to ColumnVectorType",
                                       _field->GetFieldName().c_str());
        }
        _child->data.clear();
        _parent->offsets[0] = 0;
        _convertor = std::make_unique<MultiValueAttributeConvertor<T>>(/*needHash=*/false, _field->GetFieldName(), -1,
                                                                       INDEXLIB_MULTI_VALUE_SEPARATOR_STR);
        return Status::OK();
    }
    void Append(const autil::StringView& field, uint64_t rowIdx) override;
    void Seal(uint64_t rowIdx) override;
    void Reset() override;

    orc::ListVectorBatch* TEST_getParent() { return _parent; }
    ColumnVectorType* TEST_getChild() { return _child; }

private:
    double ResizeFactor() const { return 2; }
    std::unique_ptr<MultiValueAttributeConvertor<T>> _convertor;
    orc::ListVectorBatch* _parent;
    ColumnVectorType* _child;
};

template <typename T>
void MultiValueColumnBuffer<T>::Append(const autil::StringView& field, uint64_t rowIdx)
{
    auto attrMeta = _convertor->Decode(field);
    autil::MultiValueType<T> values(attrMeta.data.data());
    if (rowIdx == _parent->capacity) {
        _parent->resize(_parent->capacity * ResizeFactor());
    }
    auto& offsets = _parent->offsets;
    offsets[rowIdx + 1] = offsets[rowIdx];
    uint32_t newValueCount = values.size();
    if (newValueCount > 0) {
        const SignedType* value = reinterpret_cast<const SignedType*>(values.data());
        _child->data.append(value, newValueCount);
        offsets[rowIdx + 1] += newValueCount;
        _parent->notNull[rowIdx] = true;
    } else {
        _parent->notNull[rowIdx] = false;
        _parent->hasNulls = true;
    }
}

template <typename T>
inline void MultiValueColumnBuffer<T>::Seal(uint64_t rowIdx)
{
    memset(_child->notNull.data(), 1, _child->notNull.size());
    _child->numElements = _parent->offsets[rowIdx];
    _parent->numElements = rowIdx;
}

template <typename T>
inline void MultiValueColumnBuffer<T>::Reset()
{
    _child->reset();
    _parent->reset();
    _child->data.clear();
}

} // namespace indexlibv2::index
