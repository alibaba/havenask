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

#include "indexlib/document/normal/Field.h"

#include "indexlib/base/Constant.h"
#include "indexlib/util/Exception.h"

using namespace std;

namespace indexlib { namespace document {

Field::Field(autil::mem_pool::Pool* pool, FieldTag fieldTag)
    : _pool(pool)
    , _fieldId(INVALID_FIELDID)
    , _fieldTag(fieldTag)
{
}

Field::~Field() {}

bool Field::operator==(const Field& field) const
{
    if (this == &field)
        return true;
    if (_fieldId != field._fieldId || _fieldTag != field._fieldTag)
        return false;
    return true;
}
}} // namespace indexlib::document
