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
#include "indexlib/index/inverted_index/config/DateIndexConfig.h"

#include "indexlib/config/ConfigDefine.h"
#include "indexlib/util/Exception.h"

using namespace std;
using indexlib::config::DateLevelFormat;

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, DateIndexConfig);

struct DateIndexConfig::Impl {
    Granularity buildGranularity = DateLevelFormat::GU_UNKOWN;
    DateLevelFormat dateLevelFormat;
    std::string defaultGranularityStr = "second";
};

DateIndexConfig::DateIndexConfig(const string& indexName, InvertedIndexType indexType)
    : SingleFieldIndexConfig(indexName, indexType)
    , _impl(make_unique<Impl>())
{
}

DateIndexConfig::DateIndexConfig(const string& indexName, InvertedIndexType indexType,
                                 const std::string& defaultGranularity)
    : SingleFieldIndexConfig(indexName, indexType)
    , _impl(make_unique<Impl>())
{
    SetDefaultGranularity(defaultGranularity);
}

DateIndexConfig::DateIndexConfig(const DateIndexConfig& other)
    : SingleFieldIndexConfig(other)
    , _impl(make_unique<Impl>(*(other._impl)))
{
}

DateIndexConfig::~DateIndexConfig() {}

void DateIndexConfig::Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const
{
    assert(json.GetMode() == autil::legacy::Jsonizable::TO_JSON);
    SingleFieldIndexConfig::Serialize(json);
    string granularityStr = GranularityToString(_impl->buildGranularity);
    json.Jsonize(BUILD_GRANULARITY, granularityStr);
}

Status DateIndexConfig::CheckEqual(const InvertedIndexConfig& other) const
{
    auto status = SingleFieldIndexConfig::CheckEqual(other);
    RETURN_IF_STATUS_ERROR(status, "date index config check equal fail, indexName[%s]", GetIndexName().c_str());
    const DateIndexConfig& other2 = (const DateIndexConfig&)other;
    CHECK_CONFIG_EQUAL(_impl->buildGranularity, other2._impl->buildGranularity, "build granularity  not equal");
    return Status::OK();
}

void DateIndexConfig::Check() const
{
    SingleFieldIndexConfig::Check();
    if (_impl->buildGranularity == DateLevelFormat::GU_UNKOWN) {
        INDEXLIB_FATAL_ERROR(Schema, "unkown granularity");
    }
    if (GetFieldConfig()->IsMultiValue()) {
        INDEXLIB_FATAL_ERROR(Schema, "date field can not be multi value field");
    }

    if (IsHashTypedDictionary()) {
        INDEXLIB_FATAL_ERROR(Schema, "date index should not set use_hash_typed_dictionary");
    }
}

InvertedIndexConfig* DateIndexConfig::Clone() const { return new DateIndexConfig(*this); }

DateIndexConfig::Granularity DateIndexConfig::GetBuildGranularity() const { return _impl->buildGranularity; }

DateLevelFormat DateIndexConfig::GetDateLevelFormat() const { return _impl->dateLevelFormat; }

void DateIndexConfig::SetBuildGranularity(Granularity granularity) { _impl->buildGranularity = granularity; }
void DateIndexConfig::SetDateLevelFormat(DateLevelFormat format) { _impl->dateLevelFormat = format; }
string DateIndexConfig::GranularityToString(DateIndexConfig::Granularity granularity) const
{
    switch (granularity) {
    case DateLevelFormat::MILLISECOND:
        return "millisecond";
    case DateLevelFormat::SECOND:
        return "second";
    case DateLevelFormat::MINUTE:
        return "minute";
    case DateLevelFormat::HOUR:
        return "hour";
    case DateLevelFormat::DAY:
        return "day";
    case DateLevelFormat::MONTH:
        return "month";
    case DateLevelFormat::YEAR:
        return "year";
    default:
        return "unkown";
    }
}

DateLevelFormat::Granularity DateIndexConfig::StringToGranularity(const string& str)
{
    if (str == "millisecond") {
        return DateLevelFormat::MILLISECOND;
    } else if (str == "second") {
        return DateLevelFormat::SECOND;
    } else if (str == "minute") {
        return DateLevelFormat::MINUTE;
    } else if (str == "hour") {
        return DateLevelFormat::HOUR;
    } else if (str == "day") {
        return DateLevelFormat::DAY;
    } else if (str == "month") {
        return DateLevelFormat::MONTH;
    } else if (str == "year") {
        return DateLevelFormat::YEAR;
    }
    return DateLevelFormat::GU_UNKOWN;
}

void DateIndexConfig::SetDefaultGranularity(const std::string& defaultGranularity)
{
    _impl->defaultGranularityStr = defaultGranularity;
    _impl->buildGranularity = StringToGranularity(defaultGranularity);
    _impl->dateLevelFormat.Init(_impl->buildGranularity);
}

void DateIndexConfig::DoDeserialize(const autil::legacy::Any& any,
                                    const config::IndexConfigDeserializeResource& resource)
{
    SingleFieldIndexConfig::DoDeserialize(any, resource);
    std::string defaultGranularity = (GetFieldConfig()->GetFieldType() == ft_date) ? "day" : "second";
    SetDefaultGranularity(defaultGranularity);
    autil::legacy::Jsonizable::JsonWrapper json(any);
    string granularityStr;
    json.Jsonize(BUILD_GRANULARITY, granularityStr, _impl->defaultGranularityStr);
    _impl->buildGranularity = StringToGranularity(granularityStr);
    if (_impl->buildGranularity == DateLevelFormat::GU_UNKOWN) {
        INDEXLIB_FATAL_ERROR(Schema, "invalid granularity [%s]", granularityStr.c_str());
    }
    _impl->dateLevelFormat.Init(_impl->buildGranularity);
}

} // namespace indexlibv2::config
