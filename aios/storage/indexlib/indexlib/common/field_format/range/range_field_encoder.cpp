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
#include "indexlib/common/field_format/range/range_field_encoder.h"

#include "indexlib/config/index_schema.h"
#include "indexlib/config/range_index_config.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::common;

namespace indexlib { namespace common {

IE_LOG_SETUP(common, RangeFieldEncoder);

RangeFieldEncoder::RangeFieldEncoder(const config::IndexSchemaPtr& indexSchema) { Init(indexSchema); }

RangeFieldEncoder::~RangeFieldEncoder() {}

void RangeFieldEncoder::Init(const IndexSchemaPtr& indexSchema)
{
    assert(indexSchema);
    auto indexConfigs = indexSchema->CreateIterator(false);
    for (auto iter = indexConfigs->Begin(); iter != indexConfigs->End(); iter++) {
        const IndexConfigPtr& indexConfig = *iter;
        if (indexConfig->GetInvertedIndexType() != it_range) {
            continue;
        }

        RangeIndexConfigPtr rangeConfig = DYNAMIC_POINTER_CAST(RangeIndexConfig, indexConfig);
        fieldid_t fieldId = rangeConfig->GetFieldConfig()->GetFieldId();
        FieldType fieldType = rangeConfig->GetFieldConfig()->GetFieldType();
        RangeFieldType rangeType = GetRangeFieldType(fieldType);
        if ((size_t)fieldId >= mFieldVec.size()) {
            mFieldVec.resize(fieldId + 1, UNKOWN);
            mFieldVec[fieldId] = rangeType;
        } else {
            mFieldVec[fieldId] = rangeType;
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
}} // namespace indexlib::common
