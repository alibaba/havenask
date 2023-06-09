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
#include "indexlib/index/common/field_format/range/RangeFieldEncoder.h"

#include "indexlib/index/inverted_index/config/RangeIndexConfig.h"

namespace indexlib::index {

AUTIL_LOG_SETUP(indexlib.index, RangeFieldEncoder);

RangeFieldEncoder::RangeFieldEncoder(const std::vector<std::shared_ptr<indexlibv2::config::IIndexConfig>>& indexConfigs)
{
    Init(indexConfigs);
}

RangeFieldEncoder::~RangeFieldEncoder() {}

void RangeFieldEncoder::Init(const std::vector<std::shared_ptr<indexlibv2::config::IIndexConfig>>& indexConfigs)
{
    for (const auto& indexConfig : indexConfigs) {
        auto rangeConfig = std::dynamic_pointer_cast<indexlibv2::config::RangeIndexConfig>(indexConfig);
        if (!rangeConfig || !rangeConfig->IsNormal() || rangeConfig->GetInvertedIndexType() != it_range) {
            continue;
        }

        fieldid_t fieldId = rangeConfig->GetFieldConfig()->GetFieldId();
        FieldType fieldType = rangeConfig->GetFieldConfig()->GetFieldType();
        RangeFieldType rangeType = GetRangeFieldType(fieldType);
        if ((size_t)fieldId >= _fieldVec.size()) {
            _fieldVec.resize(fieldId + 1, UNKOWN);
            _fieldVec[fieldId] = rangeType;
        } else {
            _fieldVec[fieldId] = rangeType;
        }
    }
}

RangeFieldEncoder::RangeFieldType RangeFieldEncoder::GetRangeFieldType(FieldType fieldType)
{
    if (fieldType == ft_int8 || fieldType == ft_int16 || fieldType == ft_int32 || fieldType == ft_int64) {
        return INT;
    }

    if (fieldType == ft_uint8 || fieldType == ft_uint16 || fieldType == ft_uint32 || fieldType == ft_uint64) {
        return UINT;
    }
    return UNKOWN;
}
} // namespace indexlib::index
