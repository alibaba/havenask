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

#include "autil/DataBuffer.h"
#include "indexlib/base/Types.h"

namespace indexlib::document {

#pragma pack(push, 2)

class Field
{
public:
    const static size_t MAX_SECTION_PER_FIELD = 256;
    const static uint8_t FIELD_DESCRIPTOR_MASK = 0x80;
    enum class FieldTag : int8_t {
        // field tag range [0, 127]
        TOKEN_FIELD = 0,
        RAW_FIELD = 1,
        NULL_FIELD = 2,
        UNKNOWN_FIELD = 127,
    };

public:
    Field(autil::mem_pool::Pool* pool, FieldTag fieldTag);
    virtual ~Field();

public:
    virtual void Reset() = 0;
    virtual void serialize(autil::DataBuffer& dataBuffer) const = 0;
    virtual void deserialize(autil::DataBuffer& dataBuffer) = 0;
    virtual bool operator==(const Field& field) const;
    virtual bool operator!=(const Field& field) const { return !(*this == field); }

public:
    fieldid_t GetFieldId() const { return _fieldId; }
    void SetFieldId(fieldid_t fieldId) { _fieldId = fieldId; }
    FieldTag GetFieldTag() const { return _fieldTag; }

protected:
    autil::mem_pool::Pool* _pool;
    fieldid_t _fieldId;
    FieldTag _fieldTag;
};

#pragma pack(pop)

} // namespace indexlib::document
