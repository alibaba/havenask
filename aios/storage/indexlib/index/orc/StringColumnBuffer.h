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

#include "autil/Log.h"
#include "indexlib/index/common/field_format/attribute/StringAttributeConvertor.h"
#include "indexlib/index/orc/ColumnBuffer.h"
#include "orc/Vector.hh"

namespace indexlibv2::index {

class StringColumnBuffer final : public ColumnBuffer
{
public:
    StringColumnBuffer(const config::FieldConfig* field, uint64_t batchSize)
        : ColumnBuffer(field, batchSize)
        , _totalLength(0)
    {
    }
    ~StringColumnBuffer() = default;

    StringColumnBuffer(const StringColumnBuffer&) = delete;
    StringColumnBuffer& operator=(const StringColumnBuffer&) = delete;
    StringColumnBuffer(StringColumnBuffer&&) = delete;
    StringColumnBuffer& operator=(StringColumnBuffer&&) = delete;

    Status Init(orc::ColumnVectorBatch* batch) override
    {
        if (_batchSize <= 0) {
            return Status::InvalidArgs("batch size should be larger than 0");
        }
        _strBatch = dynamic_cast<orc::StringVectorBatch*>(batch);
        if (_strBatch == nullptr) {
            return Status::InvalidArgs("fail to dynamic cast ColumnVectorBatch to StringVectorBatch");
        }
        _convertor = std::make_unique<StringAttributeConvertor>(/*needHash=*/false, _field->GetFieldName());
        assert(_convertor);
        return Status::OK();
    }

    void Append(const autil::StringView& field, uint64_t rowIdx) override
    {
        auto attrMeta = _convertor->Decode(field);
        autil::MultiValueType<char> values(attrMeta.data.data());
        size_t strLen = values.size();
        if (strLen > 0) {
            const char* str = values.data();
            assert(str != nullptr);
            _strBatch->blob.append(str, strLen);
            _totalLength += strLen;
        }
        _strBatch->length[rowIdx] = strLen;
        _strBatch->notNull[rowIdx] = true;
    }

    void Seal(uint64_t rowIdx) override
    {
        auto blob = _strBatch->blob.data();
        uint64_t offset = 0;
        for (uint64_t i = 0; i < rowIdx; i++) {
            _strBatch->data[i] = blob + offset;
            offset += _strBatch->length[i];
        }
        _strBatch->numElements = rowIdx;
    }

    void Reset() override
    {
        _totalLength = 0;
        _strBatch->reset();
        _strBatch->blob.clear();
    }

    orc::StringVectorBatch* TEST_getBatch() { return _strBatch; }

private:
    std::unique_ptr<StringAttributeConvertor> _convertor;
    orc::StringVectorBatch* _strBatch;
    uint64_t _totalLength;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
