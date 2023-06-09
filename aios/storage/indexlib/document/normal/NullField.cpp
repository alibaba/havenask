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
#include "indexlib/document/normal/NullField.h"

#include "indexlib/base/Constant.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

namespace indexlib { namespace document {
AUTIL_LOG_SETUP(indexlib.document, NullField);

NullField::NullField(Pool* pool) : Field(pool, FieldTag::NULL_FIELD) {}

NullField::~NullField() {}

void NullField::Reset() { _fieldId = INVALID_FIELDID; }

void NullField::serialize(autil::DataBuffer& dataBuffer) const { dataBuffer.write(_fieldId); }

void NullField::deserialize(autil::DataBuffer& dataBuffer) { dataBuffer.read(_fieldId); }

bool NullField::operator==(const Field& field) const
{
    if (!Field::operator==(field)) {
        return false;
    }

    const NullField* typedOtherField = dynamic_cast<const NullField*>(&field);
    if (typedOtherField == nullptr) {
        return false;
    }
    return true;
}
}} // namespace indexlib::document
