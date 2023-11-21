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
#include "indexlib/index/inverted_index/InvertedIndexFields.h"

#include "indexlib/document/normal/IndexRawField.h"
#include "indexlib/document/normal/IndexTokenizeField.h"
#include "indexlib/document/normal/NullField.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/util/Exception.h"

using indexlib::document::Field;
using indexlib::document::IndexRawField;
using indexlib::document::IndexTokenizeField;
using indexlib::document::NullField;

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, InvertedIndexFields);

InvertedIndexFields::InvertedIndexFields(autil::mem_pool::Pool* pool) : _pool(pool) { assert(_pool); }

InvertedIndexFields::~InvertedIndexFields()
{
    size_t size = _fields.size();
    for (size_t i = 0; i < size; i++) {
        if (_fields[i]) {
            indexlib::IE_POOL_COMPATIBLE_DELETE_CLASS(_pool, _fields[i]);
            _fields[i] = NULL;
        }
    }
    _fields.clear();
}

void InvertedIndexFields::SerializeFieldVector(autil::DataBuffer& dataBuffer, const FieldVector& fields) const
{
    uint32_t size = fields.size();
    dataBuffer.write(size);
    for (uint32_t i = 0; i < size; ++i) {
        uint8_t descriptor = GenerateFieldDescriptor(fields[i]);
        dataBuffer.write(descriptor);
        if (descriptor != 0) {
            dataBuffer.write(*(fields[i]));
        }
    }
}

uint8_t InvertedIndexFields::GenerateFieldDescriptor(const Field* field) const
{
    if (field == NULL) {
        return 0;
    }

    int8_t fieldTagNum = static_cast<int8_t>(field->GetFieldTag());
    return Field::FIELD_DESCRIPTOR_MASK | fieldTagNum;
}

void InvertedIndexFields::serialize(autil::DataBuffer& dataBuffer) const
{
    dataBuffer.write(SERIALIZE_VERSION);
    SerializeFieldVector(dataBuffer, _fields);
}
void InvertedIndexFields::deserialize(autil::DataBuffer& dataBuffer)
{
    uint32_t serializeVersion = 0;

    dataBuffer.read(serializeVersion);
    assert(serializeVersion == SERIALIZE_VERSION);
    uint32_t size = 0;
    dataBuffer.read(size);
    _fields.resize(size);
    uint32_t fieldCount = 0;
    for (uint32_t i = 0; i < size; ++i) {
        bool fieldExist = false;
        Field::FieldTag fieldTag = Field::FieldTag::TOKEN_FIELD;
        uint8_t descriptor = 0;
        dataBuffer.read(descriptor);
        fieldExist = (descriptor != 0);

        if (fieldExist) {
            fieldTag = GetFieldTagFromFieldDescriptor(descriptor);
        }

        if (!fieldExist) {
            _fields[i] = NULL;
        } else {
            _fields[i] = CreateFieldByTag(_pool, fieldTag, true);
            if (NULL == _fields[i]) {
                INDEXLIB_FATAL_ERROR(DocumentDeserialize, "invalid fieldTag[%d]", static_cast<int8_t>(fieldTag));
            }
            dataBuffer.read(*(_fields[i]));
            if (_fields[i]->GetFieldId() != INVALID_FIELDID) {
                fieldCount++;
            }
        }
    }
}

indexlib::document::Field::FieldTag InvertedIndexFields::GetFieldTagFromFieldDescriptor(uint8_t fieldDescriptor)
{
    return static_cast<Field::FieldTag>((~Field::FIELD_DESCRIPTOR_MASK) & fieldDescriptor);
}

autil::StringView InvertedIndexFields::GetIndexType() const { return INVERTED_INDEX_TYPE_STR; }

size_t InvertedIndexFields::EstimateMemory() const { return 0; }

indexlib::document::Field* InvertedIndexFields::GetField(fieldid_t fieldId) const
{
    return fieldId >= (fieldid_t)_fields.size() ? nullptr : _fields[fieldId];
}

Field* InvertedIndexFields::CreateFieldByTag(autil::mem_pool::Pool* pool, Field::FieldTag fieldTag, bool fieldHasPool)
{
    auto fieldPool = fieldHasPool ? pool : nullptr;
    if (fieldTag == Field::FieldTag::TOKEN_FIELD) {
        return IE_POOL_COMPATIBLE_NEW_CLASS(pool, IndexTokenizeField, fieldPool);
    } else if (fieldTag == Field::FieldTag::RAW_FIELD) {
        return IE_POOL_COMPATIBLE_NEW_CLASS(pool, IndexRawField, fieldPool);
    } else if (fieldTag == Field::FieldTag::NULL_FIELD) {
        return IE_POOL_COMPATIBLE_NEW_CLASS(pool, NullField, fieldPool);
    }
    AUTIL_LOG(ERROR, "invalid fieldTag:[%u]", static_cast<uint16_t>(fieldTag));
    return NULL;
}

indexlib::document::Field* InvertedIndexFields::createIndexField(fieldid_t fieldId,
                                                                 indexlib::document::Field::FieldTag fieldTag)
{
    assert(fieldId != INVALID_FIELDID);

    if ((fieldid_t)_fields.size() > fieldId) {
        if (NULL == _fields[fieldId]) {
            _fields[fieldId] = CreateFieldByTag(_pool, fieldTag, false);
            _fields[fieldId]->SetFieldId(fieldId);
            //;++_fieldCount;
        } else if (_fields[fieldId]->GetFieldId() == INVALID_FIELDID) {
            //++_fieldCount;
            _fields[fieldId]->SetFieldId(fieldId);
        }
        return _fields[fieldId];
    }

    _fields.resize(fieldId + 1, NULL);
    Field* pField = CreateFieldByTag(_pool, fieldTag, false);
    pField->SetFieldId(fieldId);
    _fields[fieldId] = pField;
    //    ++_fieldCount;
    return pField;
}

termpayload_t InvertedIndexFields::GetTermPayload(uint64_t intKey) const
{
    assert(false);
    // not impl yet
    return 0;
}

docpayload_t InvertedIndexFields::GetDocPayload(const indexlib::config::PayloadConfig& payloadConfig,
                                                uint64_t intKey) const
{
    assert(false);
    // not impl yet
    return 0;
}

} // namespace indexlibv2::index
