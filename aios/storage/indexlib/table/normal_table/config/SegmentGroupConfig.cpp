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
#include "indexlib/table/normal_table/config/SegmentGroupConfig.h"

#include "indexlib/index/attribute/expression/FunctionConfig.h"

namespace indexlibv2::table {

struct SegmentGroupConfig::Impl {
    std::vector<std::string> groups;
    std::vector<std::string> GetGroups() const;
    std::vector<std::string> GetFunctionNames() const;
};

std::vector<std::string> SegmentGroupConfig::Impl::GetGroups() const
{
    std::vector<std::string> ret = groups;
    if (!ret.empty()) {
        ret.push_back(DEFAULT_GROUP);
    }
    return ret;
}

std::vector<std::string> SegmentGroupConfig::Impl::GetFunctionNames() const
{
    std::vector<std::string> ret = groups;
    if (!ret.empty()) {
        ret.push_back(index::AttributeFunctionConfig::ALWAYS_TRUE_FUNCTION_NAME);
    }
    return ret;
}

SegmentGroupConfig::SegmentGroupConfig() : _impl(std::make_unique<Impl>()) {}
SegmentGroupConfig::SegmentGroupConfig(const SegmentGroupConfig& other) : _impl(std::make_unique<Impl>(*other._impl)) {}
SegmentGroupConfig::~SegmentGroupConfig() {}

void SegmentGroupConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("groups", _impl->groups);
}

std::vector<std::string> SegmentGroupConfig::GetGroups() const { return _impl->GetGroups(); }
std::vector<std::string> SegmentGroupConfig::GetFunctionNames() const { return _impl->GetFunctionNames(); }

void SegmentGroupConfig::TEST_SetGroups(std::vector<std::string> groups) { _impl->groups = groups; }
bool SegmentGroupConfig::IsGroupEnabled() const { return _impl->groups.size() > 0; }

} // namespace indexlibv2::table
