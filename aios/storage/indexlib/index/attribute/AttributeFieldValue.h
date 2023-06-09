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
#include "autil/NoCopyable.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/attribute/Types.h"
#include "indexlib/util/MemBuffer.h"

namespace indexlibv2::index {

class AttributeFieldValue : private autil::NoCopyable
{
public:
    AttributeFieldValue();
    ~AttributeFieldValue() = default;

public:
    union AttributeIdentifier {
        fieldid_t fieldId;
        packattrid_t packAttrId;
    };
    uint8_t* Data() const { return (uint8_t*)_buffer.GetBuffer(); }

    const autil::StringView* GetConstStringData() const { return &_fieldValue; }

    void SetDataSize(size_t size)
    {
        _size = size;
        _fieldValue = {_buffer.GetBuffer(), _size};
    }

    size_t GetDataSize() const { return _size; }

    void ReserveBuffer(size_t size) { _buffer.Reserve(size); }

    size_t BufferLength() const { return _buffer.GetBufferSize(); }

    void SetFieldId(fieldid_t fieldId) { _identifier.fieldId = fieldId; }
    fieldid_t GetFieldId() const { return _identifier.fieldId; }

    void SetPackAttrId(packattrid_t packAttrId) { _identifier.packAttrId = packAttrId; }
    packattrid_t GetPackAttrId() const { return _identifier.packAttrId; }

    void SetIsPackAttr(bool isPacked) { _isPackAttr = isPacked; }
    bool IsPackAttr() const { return _isPackAttr; }

    void SetDocId(docid_t docId) { _docId = docId; }
    docid_t GetDocId() const { return _docId; }

    void SetIsSubDocId(bool isSubDocId) { _isSubDocId = isSubDocId; }
    bool IsSubDocId() const { return _isSubDocId; }

    void SetIsNull(bool isNull) { _isNull = isNull; }
    bool IsNull() const { return _isNull; }
    void Reset();

private:
    autil::StringView _fieldValue;
    size_t _size;
    indexlib::util::MemBuffer _buffer;
    AttributeIdentifier _identifier;
    docid_t _docId;
    bool _isSubDocId;
    bool _isPackAttr;
    bool _isNull;
};

} // namespace indexlibv2::index
