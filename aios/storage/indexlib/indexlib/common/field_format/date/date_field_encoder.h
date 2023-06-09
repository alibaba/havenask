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
#ifndef __INDEXLIB_DATE_FIELD_ENCODER_H
#define __INDEXLIB_DATE_FIELD_ENCODER_H

#include <ctime>
#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/date_index_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/common/field_format/date/DateTerm.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/TimestampUtil.h"
namespace indexlib::common {

class DateFieldEncoder
{
public:
    DateFieldEncoder(const config::IndexPartitionSchemaPtr& schema);
    ~DateFieldEncoder();

public:
    void Init(const config::IndexPartitionSchemaPtr& schema);
    bool IsDateIndexField(fieldid_t fieldId);
    void Encode(fieldid_t fieldId, const std::string& fieldValue, std::vector<dictkey_t>& dictKeys);

private:
    struct FieldInfo {
        config::DateIndexConfig::Granularity granularity = config::DateLevelFormat::GU_UNKOWN;
        config::DateLevelFormat dataLevelFormat;
        FieldType fieldType = FieldType::ft_unknown;
        int32_t defaultTimeZoneDelta = 0;
    };
    std::vector<FieldInfo> mDateDescriptions;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DateFieldEncoder);

inline void DateFieldEncoder::Encode(fieldid_t fieldId, const std::string& fieldValue, std::vector<dictkey_t>& dictKeys)
{
    dictKeys.clear();
    if (!IsDateIndexField(fieldId)) {
        return;
    }
    if (fieldValue.empty()) {
        IE_LOG(DEBUG, "field [%d] value is empty.", fieldId);
        return;
    }

    uint64_t time;
    if (!util::TimestampUtil::ConvertToTimestamp(mDateDescriptions[fieldId].fieldType, autil::StringView(fieldValue),
                                                 time, mDateDescriptions[fieldId].defaultTimeZoneDelta)) {
        IE_LOG(DEBUG, "field value [%s] is invalid format for type [%s] .", fieldValue.c_str(),
               config::FieldConfig::FieldTypeToStr(mDateDescriptions[fieldId].fieldType));
        return;
    }

    util::TimestampUtil::TM date = util::TimestampUtil::ConvertToTM(time);
    config::DateLevelFormat& format = mDateDescriptions[fieldId].dataLevelFormat;
    indexlib::index::DateTerm tmp = indexlib::index::DateTerm::ConvertToDateTerm(date, format);
    indexlib::index::DateTerm::EncodeDateTermToTerms(tmp, format, dictKeys);
}

} // namespace indexlib::common

#endif //__INDEXLIB_DATE_FIELD_ENCODER_H
