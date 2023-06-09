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
#include "indexlib/config/date_index_config.h"

#include "indexlib/config/config_define.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/index/inverted_index/config/DateIndexConfig.h"

using namespace std;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, DateIndexConfig);

struct DateIndexConfig::Impl {
    Granularity buildGranularity = DateLevelFormat::GU_UNKOWN;
    DateLevelFormat dateLevelFormat;
    std::string defaultGranularityStr = "second";
    Impl() {}
    Impl(const std::string& defaultGranularity) : defaultGranularityStr(defaultGranularity) {}
};

DateIndexConfig::DateIndexConfig(const string& indexName, InvertedIndexType indexType,
                                 const std::string& defaultGranularity)
    : SingleFieldIndexConfig(indexName, indexType)
    , mImpl(make_unique<Impl>(defaultGranularity))
{
    mImpl->buildGranularity = StringToGranularity(defaultGranularity);
    mImpl->dateLevelFormat.Init(mImpl->buildGranularity);
}

DateIndexConfig::DateIndexConfig(const DateIndexConfig& other)
    : SingleFieldIndexConfig(other)
    , mImpl(make_unique<Impl>(*(other.mImpl)))
{
}

DateIndexConfig::~DateIndexConfig() {}

void DateIndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    SingleFieldIndexConfig::Jsonize(json);
    if (json.GetMode() == TO_JSON) {
        string granularityStr = GranularityToString(mImpl->buildGranularity);
        json.Jsonize(BUILD_GRANULARITY, granularityStr);
    } else {
        string granularityStr;
        json.Jsonize(BUILD_GRANULARITY, granularityStr, mImpl->defaultGranularityStr);
        mImpl->buildGranularity = StringToGranularity(granularityStr);
        mImpl->dateLevelFormat.Init(mImpl->buildGranularity);
    }
}

void DateIndexConfig::AssertEqual(const IndexConfig& other) const
{
    SingleFieldIndexConfig::AssertEqual(other);
    const DateIndexConfig& other2 = (const DateIndexConfig&)other;
    IE_CONFIG_ASSERT_EQUAL(mImpl->buildGranularity, other2.mImpl->buildGranularity, "build granularity  not equal");
}

void DateIndexConfig::AssertCompatible(const IndexConfig& other) const { AssertEqual(other); }

void DateIndexConfig::Check() const
{
    SingleFieldIndexConfig::Check();
    if (mImpl->buildGranularity == DateLevelFormat::GU_UNKOWN) {
        INDEXLIB_FATAL_ERROR(Schema, "unkown granularity");
    }
    if (GetFieldConfig()->IsMultiValue()) {
        INDEXLIB_FATAL_ERROR(Schema, "date field can not be multi value field");
    }

    if (IsHashTypedDictionary()) {
        INDEXLIB_FATAL_ERROR(Schema, "date index should not set use_hash_typed_dictionary");
    }
}

IndexConfig* DateIndexConfig::Clone() const { return new DateIndexConfig(*this); }

std::unique_ptr<indexlibv2::config::InvertedIndexConfig> DateIndexConfig::ConstructConfigV2() const
{
    auto configV2 = std::make_unique<indexlibv2::config::DateIndexConfig>(GetIndexName(), GetInvertedIndexType(),
                                                                          mImpl->defaultGranularityStr);
    if (!FulfillConfigV2(configV2.get())) {
        IE_LOG(ERROR, "construct config v2 failed");
        return nullptr;
    }
    return configV2;
}

bool DateIndexConfig::FulfillConfigV2(indexlibv2::config::InvertedIndexConfig* configV2) const
{
    if (!SingleFieldIndexConfig::FulfillConfigV2(configV2)) {
        IE_LOG(ERROR, "fulfill date index config failed");
        return false;
    }
    auto typedConfigV2 = dynamic_cast<indexlibv2::config::DateIndexConfig*>(configV2);
    assert(typedConfigV2);
    typedConfigV2->SetBuildGranularity(mImpl->buildGranularity);
    typedConfigV2->SetDateLevelFormat(mImpl->dateLevelFormat);
    return true;
}

DateIndexConfig::Granularity DateIndexConfig::GetBuildGranularity() const { return mImpl->buildGranularity; }

DateLevelFormat DateIndexConfig::GetDateLevelFormat() const { return mImpl->dateLevelFormat; }

void DateIndexConfig::SetBuildGranularity(Granularity granularity) { mImpl->buildGranularity = granularity; }
void DateIndexConfig::SetDateLevelFormat(DateLevelFormat format) { mImpl->dateLevelFormat = format; }
string DateIndexConfig::GranularityToString(DateIndexConfig::Granularity granularity)
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

}} // namespace indexlib::config
