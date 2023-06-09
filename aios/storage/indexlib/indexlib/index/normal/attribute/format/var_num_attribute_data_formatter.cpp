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
#include "indexlib/index/normal/attribute/format/var_num_attribute_data_formatter.h"

#include "indexlib/config/attribute_config.h"
#include "indexlib/config/field_type_traits.h"
#include "indexlib/config/pack_attribute_config.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, VarNumAttributeDataFormatter);

VarNumAttributeDataFormatter::VarNumAttributeDataFormatter()
    : mIsMultiString(false)
    , mFieldSizeShift(0)
    , mFixedValueCount(-1)
    , mFixedValueLength(0)
{
}

VarNumAttributeDataFormatter::~VarNumAttributeDataFormatter() {}

void VarNumAttributeDataFormatter::Init(const AttributeConfigPtr& attrConfig)
{
    mFixedValueCount = attrConfig->GetFieldConfig()->GetFixedMultiValueCount();
    if (mFixedValueCount != -1) {
        mFixedValueLength = attrConfig->GetFixLenFieldSize();
    }

    if (attrConfig->GetFieldType() != ft_string) {
        size_t fieldTypeSize = SizeOfFieldType(attrConfig->GetFieldType());
        switch (fieldTypeSize) {
        case 1:
            mFieldSizeShift = 0;
            break;
        case 2:
            mFieldSizeShift = 1;
            break;
        case 4:
            mFieldSizeShift = 2;
            break;
        case 8:
            mFieldSizeShift = 3;
            break;

        default:
            assert(false);
            mFieldSizeShift = 0;
        }
        return;
    }

    if (attrConfig->IsMultiString()) {
        mIsMultiString = true;
    } else {
        mFieldSizeShift = 0;
    }
}
}} // namespace indexlib::index
