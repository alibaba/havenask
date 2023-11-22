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
#include "indexlib/config/SortDescription.h"

#include <map>

#include "autil/Span.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/legacy/any.h"
#include "autil/legacy/legacy_jsonizable.h"

namespace indexlibv2 { namespace config {

struct SortDescription::Impl {
    std::string sortFieldName;
    SortPattern sortPattern = config::sp_desc;
};

SortDescription::SortDescription() : _impl(std::make_unique<SortDescription::Impl>()) {}
SortDescription::SortDescription(const std::string& sortFieldName, SortPattern sortPattern)
    : _impl(std::make_unique<SortDescription::Impl>())
{
    _impl->sortFieldName = sortFieldName;
    _impl->sortPattern = sortPattern;
}
SortDescription::~SortDescription() {}
SortDescription::SortDescription(const SortDescription& other)
    : _impl(std::make_unique<SortDescription::Impl>(*other._impl))
{
}

SortDescription& SortDescription::operator=(const SortDescription& other)
{
    if (this != &other) {
        _impl = std::make_unique<SortDescription::Impl>(*other._impl);
    }
    return *this;
}

void SortDescription::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    // TODO: sort_pattern & sort_field is prefer, and support "+XXX;-YYY"
    json.Jsonize("SortField", _impl->sortFieldName, _impl->sortFieldName);
    if (json.GetMode() == TO_JSON) {
        std::string sortPatternStr = SortPatternToString(_impl->sortPattern);
        json.Jsonize("SortPattern", sortPatternStr);
    } else {
        // for compatible
        std::map<std::string, autil::legacy::Any> jsonMap = json.GetMap();
        if (jsonMap.find("sort_field") != jsonMap.end()) {
            json.Jsonize("sort_field", _impl->sortFieldName);
        }

        // sort pattern
        std::string sortPatternStr;
        json.Jsonize("SortPattern", sortPatternStr, sortPatternStr);
        if (jsonMap.find("sort_pattern") != jsonMap.end()) {
            json.Jsonize("sort_pattern", sortPatternStr);
        }
        _impl->sortPattern = SortPatternFromString(sortPatternStr);
    }
}

bool SortDescription::operator==(const SortDescription& other) const
{
    return _impl->sortFieldName == other._impl->sortFieldName && _impl->sortPattern == other._impl->sortPattern;
}
bool SortDescription::operator!=(const SortDescription& other) const { return !(*this == other); }

std::string SortDescription::ToString() const { return ToJsonString(*this); }

const std::string& SortDescription::GetSortFieldName() const { return _impl->sortFieldName; }

const SortPattern& SortDescription::GetSortPattern() const { return _impl->sortPattern; }

void SortDescription::TEST_SetSortFieldName(const std::string& fieldName) { _impl->sortFieldName = fieldName; }
void SortDescription::TEST_SetSortPattern(const SortPattern& sortPattern) { _impl->sortPattern = sortPattern; }

std::vector<SortDescription> SortDescription::TEST_Create(const std::string& descriptions)
{
    static constexpr char DELIM = ';';
    auto strVec = autil::StringTokenizer::tokenize(autil::StringView(descriptions), DELIM);
    std::vector<SortDescription> sortDescriptions;
    for (const auto& str : strVec) {
        char sign = str[0];
        if (sign == '+') {
            sortDescriptions.push_back(SortDescription(str.substr(1), sp_asc));
        } else if (sign == '-') {
            sortDescriptions.push_back(SortDescription(str.substr(1), sp_desc));
        } else {
            sortDescriptions.push_back(SortDescription(str, sp_desc));
        }
    }
    return sortDescriptions;
}

SortPattern SortDescription::SortPatternFromString(const std::string& sortPatternStr)
{
    std::string upperCaseStr = sortPatternStr;
    autil::StringUtil::toUpperCase(upperCaseStr);
    if (upperCaseStr == ASC_SORT_PATTERN) {
        return config::sp_asc;
    }
    return config::sp_desc;
}

std::string SortDescription::SortPatternToString(const SortPattern& sortPattern)
{
    if (sortPattern == config::sp_desc) {
        return DESC_SORT_PATTERN;
    } else if (sortPattern == config::sp_asc) {
        return ASC_SORT_PATTERN;
    }
    return "";
}

}} // namespace indexlibv2::config
