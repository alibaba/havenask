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
#include "indexlib/document/normal/SummaryDocument.h"

#include <iostream>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

namespace indexlib::document {
AUTIL_LOG_SETUP(indexlib.document, SummaryDocument);

const StringView& SummaryDocument::GetField(fieldid_t id) const
{
    if (id != INVALID_FIELDID && id < (fieldid_t)_fields.size()) {
        return _fields[id];
    }
    return StringView::empty_instance();
}

bool SummaryDocument::operator==(const SummaryDocument& right) const
{
    return _fields == right._fields && _notEmptyFieldCount == right._notEmptyFieldCount &&
           _firstNotEmptyFieldId == right._firstNotEmptyFieldId && _docId == right._docId;
}

bool SummaryDocument::operator!=(const SummaryDocument& right) const { return !operator==(right); }

void SummaryDocument::ClearFields(const vector<fieldid_t>& fieldIds)
{
    for (size_t i = 0; i < fieldIds.size(); ++i) {
        fieldid_t fid = fieldIds[i];
        if ((size_t)fid < _fields.size()) {
            _fields[fid] = StringView::empty_instance();
        }
    }
    _notEmptyFieldCount = 0;
    _firstNotEmptyFieldId = INVALID_FIELDID;

    for (size_t i = 0; i < _fields.size(); ++i) {
        if (_fields[i].empty()) {
            continue;
        }
        ++_notEmptyFieldCount;
        if (_firstNotEmptyFieldId == INVALID_FIELDID) {
            _firstNotEmptyFieldId = i;
        }
    }
    if (_notEmptyFieldCount == 0) {
        Reset();
    }
}

void SummaryDocument::serialize(DataBuffer& dataBuffer) const
{
    bool partialSerialize = NeedPartialSerialize();
    dataBuffer.write(partialSerialize);

    if (!partialSerialize) {
        // all fields serialize
        dataBuffer.write(_fields);
        return;
    }

    // partial serialize
    dataBuffer.write(_notEmptyFieldCount);
    for (uint32_t i = _fields.size(); i > 0; --i) {
        // reverse sequence, deserialize resize _fields once
        uint32_t fieldId = i - 1;
        const StringView& value = _fields[fieldId];
        if (partialSerialize) {
            if (value.empty()) {
                continue;
            }
            dataBuffer.write(fieldId);
        }
        dataBuffer.write(value);
    }
}

void SummaryDocument::deserialize(DataBuffer& dataBuffer, Pool* pool)
{
    Reset();
    bool partialSerialize;
    dataBuffer.read(partialSerialize);

    if (partialSerialize) {
        DeserializeNotEmptyFields(dataBuffer, pool);
    } else {
        DeserializeAllFields(dataBuffer, pool);
    }
}

void SummaryDocument::deserializeLegacyFormat(DataBuffer& dataBuffer, Pool* pool, uint32_t docVersion)
{
    deserialize(dataBuffer, pool);
}

void SummaryDocument::DeserializeAllFields(DataBuffer& dataBuffer, Pool* pool)
{
    uint32_t fieldCount;
    dataBuffer.read(fieldCount);
    _fields.resize(fieldCount);

    uint32_t emptyFieldCount = 0;
    for (uint32_t i = 0; i < fieldCount; ++i) {
        StringView value;
        dataBuffer.read(value, pool);
        _fields[i] = value;

        if (value.empty()) {
            ++emptyFieldCount;
        } else if (_firstNotEmptyFieldId == INVALID_FIELDID) {
            _firstNotEmptyFieldId = (fieldid_t)i;
        }
    }
    _notEmptyFieldCount = fieldCount - emptyFieldCount;
}

void SummaryDocument::DeserializeNotEmptyFields(DataBuffer& dataBuffer, Pool* pool)
{
    uint32_t fieldCount;
    dataBuffer.read(fieldCount);

    for (uint32_t i = fieldCount; i > 0; --i) {
        uint32_t fieldId = INVALID_FIELDID;
        dataBuffer.read(fieldId);
        if (fieldId >= (uint32_t)_fields.size()) {
            _fields.resize(fieldId + 1);
        }

        StringView value;
        dataBuffer.read(value, pool);
        _fields[fieldId] = value;

        if (!value.empty()) {
            _firstNotEmptyFieldId = fieldId;
        }
    }
    _notEmptyFieldCount = fieldCount;
}
} // namespace indexlib::document
