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
#include "indexlib/common/field_format/customized_index/customized_index_field_encoder.h"

#include "indexlib/config/customized_index_config.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace common {

IE_LOG_SETUP(common, CustomizedIndexFieldEncoder);

CustomizedIndexFieldEncoder::CustomizedIndexFieldEncoder(const config::IndexPartitionSchemaPtr& schema)
{
    Init(schema);
}

CustomizedIndexFieldEncoder::~CustomizedIndexFieldEncoder() {}

void CustomizedIndexFieldEncoder::Init(const IndexPartitionSchemaPtr& schema)
{
    assert(schema);
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    assert(indexSchema);
    auto indexConfigs = indexSchema->CreateIterator(false);
    for (auto iter = indexConfigs->Begin(); iter != indexConfigs->End(); iter++) {
        const IndexConfigPtr& indexConfig = *iter;
        if (indexConfig->GetInvertedIndexType() != it_customized) {
            continue;
        }

        CustomizedIndexConfigPtr customizedConfig = DYNAMIC_POINTER_CAST(CustomizedIndexConfig, indexConfig);
        auto fieldsIter = customizedConfig->CreateIterator();
        while (fieldsIter.HasNext()) {
            auto fieldConfigPtr = fieldsIter.Next();
            fieldid_t fieldId = fieldConfigPtr->GetFieldId();
            FieldType fieldType = fieldConfigPtr->GetFieldType();
            FloatFieldType floatType = GetFloatFieldType(fieldType);
            if ((size_t)fieldId >= mFieldVec.size()) {
                mFieldVec.resize(fieldId + 1, UNKOWN);
                mFieldVec[fieldId] = floatType;
            } else {
                mFieldVec[fieldId] = floatType;
            }
        }
    }
}

CustomizedIndexFieldEncoder::FloatFieldType CustomizedIndexFieldEncoder::GetFloatFieldType(FieldType fieldType)
{
    if (fieldType == ft_float) {
        return FLOAT;
    }

    if (fieldType == ft_double) {
        return DOUBLE;
    }
    return UNKOWN;
}

}} // namespace indexlib::common
