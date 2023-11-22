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
#include "indexlib/document/normal/FieldMetaDocument.h"

namespace indexlib::document {
AUTIL_LOG_SETUP(indexlib.document, FieldMetaDocument);

void FieldMetaDocument::SetField(fieldid_t id, const autil::StringView& value, bool isNull)
{
    if (isNull) {
        _nullFields.insert(id);
        return;
    }
    _fieldMetaDoc.SetField(id, value);
}

void FieldMetaDocument::serialize(autil::DataBuffer& dataBuffer) const
{
    _fieldMetaDoc.serialize(dataBuffer);
    dataBuffer.write(_nullFields);
}

void FieldMetaDocument::deserialize(autil::DataBuffer& dataBuffer, autil::mem_pool::Pool* pool, uint32_t docVersion)
{
    _fieldMetaDoc.deserialize(dataBuffer, pool);
    dataBuffer.read(_nullFields, pool);
}

bool FieldMetaDocument::operator==(const FieldMetaDocument& right) const
{
    return _fieldMetaDoc == right._fieldMetaDoc && _nullFields == right._nullFields;
}

bool FieldMetaDocument::operator!=(const FieldMetaDocument& right) const { return !(operator==(right)); }

const autil::StringView& FieldMetaDocument::GetField(fieldid_t id, bool& isNull) const
{
    auto iter = _nullFields.find(id);
    if (iter != _nullFields.end()) {
        isNull = true;
        return autil::StringView::empty_instance();
    }
    isNull = false;
    return _fieldMetaDoc.GetField(id);
}
} // namespace indexlib::document
