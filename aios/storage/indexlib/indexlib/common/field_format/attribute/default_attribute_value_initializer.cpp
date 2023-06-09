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
#include "indexlib/common/field_format/attribute/default_attribute_value_initializer.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

namespace indexlib { namespace common {
IE_LOG_SETUP(common, DefaultAttributeValueInitializer);

DefaultAttributeValueInitializer::DefaultAttributeValueInitializer(const string& valueStr) : mValueStr(valueStr) {}

DefaultAttributeValueInitializer::~DefaultAttributeValueInitializer() {}

bool DefaultAttributeValueInitializer::GetInitValue(docid_t docId, char* buffer, size_t bufLen) const
{
    assert(bufLen >= mValueStr.size());
    memcpy(buffer, mValueStr.c_str(), mValueStr.size());
    return true;
}

bool DefaultAttributeValueInitializer::GetInitValue(docid_t docId, StringView& value, Pool* memPool) const
{
    value = GetDefaultValue(memPool);
    return true;
}

StringView DefaultAttributeValueInitializer::GetDefaultValue(Pool* memPool) const
{
    return autil::MakeCString(mValueStr.c_str(), mValueStr.length(), memPool);
}
}} // namespace indexlib::common
