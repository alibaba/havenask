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
#include "indexlib/index/attribute/AttributeFieldValue.h"

#include "indexlib/base/Constant.h"

namespace indexlibv2::index {

AttributeFieldValue::AttributeFieldValue() { Reset(); }

void AttributeFieldValue::Reset()
{
    _identifier.fieldId = INVALID_FIELDID;
    _size = 0;
    _docId = INVALID_DOCID;
    _isSubDocId = false;
    _fieldValue = {_buffer.GetBuffer(), _size};
    _isPackAttr = false;
    _isNull = false;
}

// AttributeFieldValue::AttributeFieldValue(const AttributeFieldValue& other)
// {
//     ReserveBuffer(other._buffer.GetBufferSize());
//     _identifier = other._identifier;
//     _size = other._size;
//     if (_buffer.GetBuffer() != NULL) {
//         memcpy(_buffer.GetBuffer(), other._buffer.GetBuffer(), other._size);
//     }
//     _docId = other._docId;
//     _isSubDocId = other._isSubDocId;
//     _fieldValue = {(char*)_buffer.GetBuffer(), _size};
//     _isPackAttr = other._isPackAttr;
//     _isNull = other._isNull;
// }
} // namespace indexlibv2::index
