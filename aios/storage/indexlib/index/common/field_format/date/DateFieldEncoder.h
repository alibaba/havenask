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
#pragma once
#include <ctime>
#include <memory>

#include "indexlib/index/common/field_format/date/DateTerm.h"
#include "indexlib/index/inverted_index/config/DateIndexConfig.h"
#include "indexlib/util/TimestampUtil.h"

namespace indexlib::index {

class DateFieldEncoder
{
public:
    // inverted index configs
    DateFieldEncoder(const std::vector<std::shared_ptr<indexlibv2::config::IIndexConfig>>& indexConfigs);
    ~DateFieldEncoder();

public:
    // inverted index configs
    void Init(const std::vector<std::shared_ptr<indexlibv2::config::IIndexConfig>>& indexConfigs);
    bool IsDateIndexField(fieldid_t fieldId);
    void Encode(fieldid_t fieldId, const std::string& fieldValue, std::vector<dictkey_t>& dictKeys);

private:
    struct FieldInfo {
        indexlibv2::config::DateIndexConfig::Granularity granularity = config::DateLevelFormat::GU_UNKOWN;
        config::DateLevelFormat dataLevelFormat;
        FieldType fieldType = FieldType::ft_unknown;
        int32_t defaultTimeZoneDelta = 0;
    };
    std::vector<FieldInfo> _dateDescriptions;

private:
    AUTIL_LOG_DECLARE();
};

inline void DateFieldEncoder::Encode(fieldid_t fieldId, const std::string& fieldValue, std::vector<dictkey_t>& dictKeys)
{
    dictKeys.clear();
    if (!IsDateIndexField(fieldId)) {
        return;
    }
    if (fieldValue.empty()) {
        AUTIL_LOG(DEBUG, "field [%d] value is empty.", fieldId);
        return;
    }

    uint64_t time;
    if (!util::TimestampUtil::ConvertToTimestamp(_dateDescriptions[fieldId].fieldType, autil::StringView(fieldValue),
                                                 time, _dateDescriptions[fieldId].defaultTimeZoneDelta)) {
        AUTIL_LOG(DEBUG, "field value [%s] is invalid format for type [%s] .", fieldValue.c_str(),
                  indexlibv2::config::FieldConfig::FieldTypeToStr(_dateDescriptions[fieldId].fieldType));
        return;
    }

    util::TimestampUtil::TM date = util::TimestampUtil::ConvertToTM(time);
    config::DateLevelFormat& format = _dateDescriptions[fieldId].dataLevelFormat;
    DateTerm tmp = DateTerm::ConvertToDateTerm(date, format);
    DateTerm::EncodeDateTermToTerms(tmp, format, dictKeys);
}
} // namespace indexlib::index
