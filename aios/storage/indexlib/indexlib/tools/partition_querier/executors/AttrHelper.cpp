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
#include "indexlib/indexlib/tools/partition_querier/executors/AttrHelper.h"

#include "autil/ConstString.h"
#include "autil/MultiValueType.h"
#include "indexlib/index/common/FieldTypeTraits.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;

namespace indexlib::tools {

#define FILL_SIMPLE_ATTRIBUTE(ft, strValueType)                                                                        \
    case ft: {                                                                                                         \
        attrValue.set_type(indexlib::index::AttrTypeTraits2<ft>::valueType);                                           \
        typedef FieldTypeTraits<ft>::AttrItemType type;                                                                \
        type* realValue = (type*)(value.data());                                                                       \
        attrValue.set_##strValueType(*realValue);                                                                      \
        break;                                                                                                         \
    }

void AttrHelper::GetKvValue(const autil::StringView& value, FieldType fieldType, indexlibv2::base::AttrValue& attrValue)
{
    switch (fieldType) {
        FILL_SIMPLE_ATTRIBUTE(ft_int8, int32_value)
        FILL_SIMPLE_ATTRIBUTE(ft_uint8, uint32_value)
        FILL_SIMPLE_ATTRIBUTE(ft_int16, int32_value)
        FILL_SIMPLE_ATTRIBUTE(ft_uint16, uint32_value)
        FILL_SIMPLE_ATTRIBUTE(ft_int32, int32_value)
        FILL_SIMPLE_ATTRIBUTE(ft_uint32, uint32_value)
        FILL_SIMPLE_ATTRIBUTE(ft_int64, int64_value)
        FILL_SIMPLE_ATTRIBUTE(ft_uint64, uint64_value)
        FILL_SIMPLE_ATTRIBUTE(ft_float, float_value)
        FILL_SIMPLE_ATTRIBUTE(ft_double, double_value)
    default:
        // other FieldType should not appear hear
        break;
    }
#undef FILL_SIMPLE_ATTRIBUTE
}

vector<string> AttrHelper::ValidateAttrs(const vector<string>& attrs, const vector<string>& inputAttrs)
{
    vector<string> validatedAttrs;
    for (auto attr : inputAttrs) {
        if (find(attrs.begin(), attrs.end(), attr) != attrs.end()) {
            validatedAttrs.push_back(attr);
        }
    }
    if (validatedAttrs.empty()) {
        validatedAttrs = attrs;
    }
    return validatedAttrs;
}

} // namespace indexlib::tools
