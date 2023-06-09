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
#include "indexlib/document/normal/IndexRawField.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

namespace indexlib { namespace document {
AUTIL_LOG_SETUP(indexlib.document, IndexRawField);

IndexRawField::IndexRawField(Pool* pool) : Field(pool, FieldTag::RAW_FIELD) {}

IndexRawField::~IndexRawField() {}

void IndexRawField::Reset() { assert(false); }

void IndexRawField::serialize(autil::DataBuffer& dataBuffer) const
{
    dataBuffer.write(_data);
    dataBuffer.write(_fieldId);
}

void IndexRawField::deserialize(autil::DataBuffer& dataBuffer)
{
    StringView value;
    dataBuffer.read(value, _pool);
    _data = value;
    dataBuffer.read(_fieldId);
}

bool IndexRawField::operator==(const Field& field) const
{
    if (!Field::operator==(field)) {
        return false;
    }
    const IndexRawField* typedOtherField = dynamic_cast<const IndexRawField*>(&field);
    if (typedOtherField == nullptr) {
        return false;
    }

    return _data == typedOtherField->_data;
}
}} // namespace indexlib::document
