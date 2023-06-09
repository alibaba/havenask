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
#include <assert.h>

#include "indexlib/index/orc/TypeTraits.h"

namespace indexlibv2::index {

template <orc::TypeKind kind>
class OrcColumnVectorBatchIterator
{
public:
    using CppType = typename OrcKindTypeTraits<kind>::CppType;
    using VectorBatchType = typename OrcKindTypeTraits<kind>::VectorBatchType;

public:
    OrcColumnVectorBatchIterator(orc::ColumnVectorBatch* columnBatch)
        : _columnBatch(dynamic_cast<VectorBatchType*>(columnBatch))
    {
    }

public:
    size_t Size() const { return _columnBatch->numElements; }

    CppType operator[](size_t idx) const
    {
        assert(idx < Size());
        if constexpr (kind == orc::STRING) {
            return CppType(_columnBatch->data[idx], _columnBatch->length[idx]);
        } else {
            return _columnBatch->data[idx];
        }
    }

private:
    VectorBatchType* _columnBatch = nullptr;
};

template <orc::TypeKind kind>
class OrcListVectorBatchIterator
{
public:
    using CppType = typename OrcKindTypeTraits<kind>::CppType;
    using VectorBatchType = typename OrcKindTypeTraits<kind>::VectorBatchType;

public:
    OrcListVectorBatchIterator(orc::ColumnVectorBatch* batch) : _batch(dynamic_cast<orc::ListVectorBatch*>(batch)) {}

public:
    size_t ListSize() const { return _batch->numElements; }

    size_t ElementSize() const
    {
        const auto typedBatch = dynamic_cast<const VectorBatchType*>(_batch->elements.get());
        return typedBatch->numElements;
    }

    CppType Get(uint64_t i, uint64_t j) const
    {
        assert(i < ListSize());
        assert(j < ElementSize());

        const auto offsets = _batch->offsets.data();
        const auto typedBatch = dynamic_cast<const VectorBatchType*>(_batch->elements.get());

        int64_t offset = offsets[i];
        if constexpr (kind == orc::STRING) {
            const char* const* start = typedBatch->data.data() + offset;
            const int* length = typedBatch->length.data() + offset;
            return CppType(start[j], length[j]);
        } else {
            const CppType* start = (const CppType*)typedBatch->data.data() + offset;
            return start[j];
        }
    }

private:
    orc::ListVectorBatch* _batch = nullptr;
};

} // namespace indexlibv2::index
