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
#include "indexlib/common/field_format/date/date_field_encoder.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::util;
namespace indexlib::common {
IE_LOG_SETUP(common, DateFieldEncoder);

DateFieldEncoder::DateFieldEncoder(const config::IndexPartitionSchemaPtr& schema) { Init(schema); }

DateFieldEncoder::~DateFieldEncoder() {}

void DateFieldEncoder::Init(const IndexPartitionSchemaPtr& schema)
{
    assert(schema);
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    assert(indexSchema);
    auto indexConfigs = indexSchema->CreateIterator(false);
    for (auto iter = indexConfigs->Begin(); iter != indexConfigs->End(); iter++) {
        const IndexConfigPtr& indexConfig = *iter;
        if (indexConfig->GetInvertedIndexType() != it_datetime) {
            continue;
        }
        DateIndexConfigPtr dateConfig = DYNAMIC_POINTER_CAST(DateIndexConfig, indexConfig);
        assert(dateConfig);
        fieldid_t fieldId = dateConfig->GetFieldConfig()->GetFieldId();
        if (fieldId >= (fieldid_t)mDateDescriptions.size()) {
            mDateDescriptions.resize(fieldId + 1);
        }

        FieldInfo& fieldInfo = mDateDescriptions[fieldId];
        fieldInfo.granularity = dateConfig->GetBuildGranularity();
        fieldInfo.dataLevelFormat = dateConfig->GetDateLevelFormat();
        fieldInfo.fieldType = dateConfig->GetFieldConfig()->GetFieldType();
        fieldInfo.defaultTimeZoneDelta = dateConfig->GetFieldConfig()->GetDefaultTimeZoneDelta();
    }
}

bool DateFieldEncoder::IsDateIndexField(fieldid_t fieldId)
{
    if (fieldId >= (fieldid_t)mDateDescriptions.size()) {
        return false;
    }

    return mDateDescriptions[fieldId].granularity != DateLevelFormat::GU_UNKOWN;
}

} // namespace indexlib::common
