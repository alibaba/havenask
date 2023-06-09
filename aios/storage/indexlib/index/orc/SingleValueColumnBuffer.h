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

#include "autil/ConstString.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "indexlib/index/orc/ColumnBuffer.h"
#include "indexlib/index/orc/TypeTraits.h"
#include "orc/Vector.hh"

namespace indexlibv2::index {

template <typename T>
class SingleValueColumnBuffer final : public ColumnBuffer
{
public:
    using ColumnVectorType = typename OrcTypeTraits<T>::ColumnVectorType;

    SingleValueColumnBuffer(const config::FieldConfig* field, uint64_t batchSize)

        : ColumnBuffer(field, batchSize)
    {
        _defaultValue = autil::StringUtil::fromString<T>(_field->GetDefaultValue());
    }

    SingleValueColumnBuffer(const SingleValueColumnBuffer&) = delete;
    SingleValueColumnBuffer& operator=(const SingleValueColumnBuffer&) = delete;
    SingleValueColumnBuffer(SingleValueColumnBuffer&&) = delete;
    SingleValueColumnBuffer& operator=(SingleValueColumnBuffer&&) = delete;

    ~SingleValueColumnBuffer() = default;

    Status Init(orc::ColumnVectorBatch* batch) override
    {
        _batch = dynamic_cast<ColumnVectorType*>(batch);
        if (_batch == nullptr) {
            AUTIL_LOG(ERROR, "SingleValueColumnBuffer init failed, dynamic cast returns nullptr");
            return Status::InvalidArgs("SingleValueColumnBuffer init failed, dynamic cast returns nullptr");
        }
        return Status::OK();
    }
    void Append(const autil::StringView& field, uint64_t rowIdx) override { _batch->data[rowIdx] = Decode(field); }
    void Seal(uint64_t rowIdx) override { _batch->numElements = rowIdx; }
    void Reset() override { _batch->reset(); }

    ColumnVectorType* TEST_getBatch() { return _batch; }

private:
    T Decode(const autil::StringView& field)
    {
        if (field.size() != sizeof(T)) {
            return _defaultValue;
        }
        return *reinterpret_cast<const T*>(field.data());
    }
    T _defaultValue;
    ColumnVectorType* _batch;

    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SingleValueColumnBuffer, T);
} // namespace indexlibv2::index
