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
#include "indexlib/config/disable_fields_config.h"

using namespace std;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, DisableFieldsConfig);

const string DisableFieldsConfig::SUMMARY_FIELD_ALL = "__SUMMARY_FIELD_ALL__";
const string DisableFieldsConfig::SUMMARY_FIELD_NONE = "__SUMMARY_FIELD_NONE__";

const string DisableFieldsConfig::SOURCE_FIELD_ALL = "__SOURCE_FIELD_ALL__";
const string DisableFieldsConfig::SOURCE_FIELD_NONE = "__SOURCE_FIELD_NONE__";
const string DisableFieldsConfig::SOURCE_FIELD_GROUP = "__SOURCE_FIELD_GROUP__:";

DisableFieldsConfig::DisableFieldsConfig() {}

DisableFieldsConfig::~DisableFieldsConfig() {}

void DisableFieldsConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("attributes", attributes, attributes);
    json.Jsonize("indexes", indexes, indexes);
    json.Jsonize("pack_attributes", packAttributes, packAttributes);
    json.Jsonize("rewrite_load_config", rewriteLoadConfig, rewriteLoadConfig);
    if (json.GetMode() == TO_JSON) {
        string summaryDisableField = DisableSummaryToStr(summarys);
        json.Jsonize("summarys", summaryDisableField);

        string sourceDisableField = DisableSourceToStr(sources);
        json.Jsonize("sources", sourceDisableField);
    } else {
        string summaryDisableField;
        json.Jsonize("summarys", summaryDisableField, SUMMARY_FIELD_NONE);
        summarys = StrToDisableSummary(summaryDisableField);

        string sourceDisableField;
        json.Jsonize("sources", sourceDisableField, SOURCE_FIELD_NONE);
        sources = StrToDisableSource(sourceDisableField);
    }
}

bool DisableFieldsConfig::operator==(const DisableFieldsConfig& other) const
{
    return attributes == other.attributes && packAttributes == other.packAttributes && indexes == other.indexes &&
           summarys == other.summarys && sources == other.sources;
}

string DisableFieldsConfig::DisableSummaryToStr(DisableFieldsConfig::SummaryDisableField summarys) const
{
    if (summarys == SummaryDisableField::SDF_FIELD_ALL) {
        return SUMMARY_FIELD_ALL;
    }
    return SUMMARY_FIELD_NONE;
}

DisableFieldsConfig::SummaryDisableField DisableFieldsConfig::StrToDisableSummary(const string& str) const
{
    if (str == SUMMARY_FIELD_ALL) {
        return DisableFieldsConfig::SummaryDisableField::SDF_FIELD_ALL;
    }
    return DisableFieldsConfig::SummaryDisableField::SDF_FIELD_NONE;
}

string DisableFieldsConfig::DisableSourceToStr(DisableFieldsConfig::SourceDisableField summarys) const
{
    if (sources == SourceDisableField::CDF_FIELD_ALL) {
        return SOURCE_FIELD_ALL;
    }
    if (sources == SourceDisableField::CDF_FIELD_GROUP) {
        return SOURCE_FIELD_GROUP + autil::StringUtil::toString(disabledSourceGroupIds, ",");
    }
    return SOURCE_FIELD_NONE;
}

DisableFieldsConfig::SourceDisableField DisableFieldsConfig::StrToDisableSource(const string& str)
{
    disabledSourceGroupIds.clear();
    if (str == SOURCE_FIELD_ALL) {
        return DisableFieldsConfig::SourceDisableField::CDF_FIELD_ALL;
    }

    if (str.find(SOURCE_FIELD_GROUP) == 0) {
        string groupIdStr = str.substr(SOURCE_FIELD_GROUP.size());
        vector<index::sourcegroupid_t> groupIds;
        autil::StringUtil::fromString(groupIdStr, groupIds, ",");
        disabledSourceGroupIds = groupIds;
        return DisableFieldsConfig::SourceDisableField::CDF_FIELD_GROUP;
    }
    return DisableFieldsConfig::SourceDisableField::CDF_FIELD_NONE;
}
}} // namespace indexlib::config
