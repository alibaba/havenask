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
#include "indexlib/index/attribute/format/MultiValueAttributeDataFormatter.h"

#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/FieldTypeTraits.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, MultiValueAttributeDataFormatter);

MultiValueAttributeDataFormatter::MultiValueAttributeDataFormatter()
    : _isMultiString(false)
    , _fieldSizeShift(0)
    , _fixedValueCount(-1)
    , _fixedValueLength(0)
{
}

void MultiValueAttributeDataFormatter::Init(const std::shared_ptr<config::AttributeConfig>& attrConfig)
{
    _fixedValueCount = attrConfig->GetFieldConfig()->GetFixedMultiValueCount();
    if (_fixedValueCount != -1) {
        _fixedValueLength = attrConfig->GetFixLenFieldSize();
    }

    if (attrConfig->GetFieldType() != ft_string) {
        size_t fieldTypeSize = indexlib::index::SizeOfFieldType(attrConfig->GetFieldType());
        switch (fieldTypeSize) {
        case 1:
            _fieldSizeShift = 0;
            break;
        case 2:
            _fieldSizeShift = 1;
            break;
        case 4:
            _fieldSizeShift = 2;
            break;
        case 8:
            _fieldSizeShift = 3;
            break;

        default:
            assert(false);
            _fieldSizeShift = 0;
        }
        return;
    }

    if (attrConfig->IsMultiString()) {
        _isMultiString = true;
    } else {
        _fieldSizeShift = 0;
    }
}
} // namespace indexlibv2::index
