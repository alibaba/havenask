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
#include "indexlib/index/common/field_format/attribute/MultiStringAttributeConvertor.h"
#include "indexlib/index/orc/ColumnBuffer.h"
#include "orc/Vector.hh"

namespace indexlibv2::index {

class MultiStringColumnBuffer final : public ColumnBuffer
{
public:
    MultiStringColumnBuffer(const config::FieldConfig* fieldConfig, uint64_t batchSize);

    MultiStringColumnBuffer(const MultiStringColumnBuffer&) = delete;
    MultiStringColumnBuffer& operator=(const MultiStringColumnBuffer&) = delete;
    MultiStringColumnBuffer(MultiStringColumnBuffer&&) = delete;
    MultiStringColumnBuffer& operator=(MultiStringColumnBuffer&&) = delete;

    ~MultiStringColumnBuffer() = default;

    Status Init(orc::ColumnVectorBatch* batch) override;
    void Append(const autil::StringView& field, uint64_t rowIdx) override;
    void Seal(uint64_t rowIdx) override
    {
        // resize blob may change blob base ptr, set each string's pointer only at sealing.
        SetEachStrPtr();
        _parent->numElements = rowIdx;
        _child->numElements = _strCount;
    }
    void Reset() override
    {
        _strCount = 0;
        _totalLength = 0;
        _child->reset();
        _parent->reset();
        _child->blob.clear();
    }

    orc::ListVectorBatch* TEST_getParent() { return _parent; }
    orc::StringVectorBatch* TEST_getChild() { return _child; }

private:
    void SetEachStrPtr()
    {
        char* blob = _child->blob.data();
        uint64_t offset = 0;
        // _child->data holds each string's pointer
        for (uint64_t i = 0; i < _strCount; i++) {
            _child->data[i] = blob + offset;
            offset += _child->length[i];
        }
    }
    bool MaybeResizeChild()
    {
        if (_strCount >= _child->capacity) {
            _child->resize(_child->capacity * ResizeFactor());
            return true;
        }
        return false;
    }
    double ResizeFactor() const { return 2; }

    std::unique_ptr<MultiStringAttributeConvertor> _convertor;
    orc::ListVectorBatch* _parent;
    orc::StringVectorBatch* _child;
    uint64_t _strCount;
    uint64_t _totalLength;
};

} // namespace indexlibv2::index
