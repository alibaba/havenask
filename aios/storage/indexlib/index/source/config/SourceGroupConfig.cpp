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
#include "indexlib/index/source/config/SourceGroupConfig.h"

#include "indexlib/base/Constant.h"
#include "indexlib/config/ConfigDefine.h"
#include "indexlib/config/GroupDataParameter.h"
#include "indexlib/index/source/Constant.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, SourceGroupConfig);

const std::string SourceGroupConfig::SOURCE_GROUP_CONFIG_FIELD_MODE = "field_mode";
const std::string SourceGroupConfig::SOURCE_GROUP_CONFIG_FIELDS = "fields";
const std::string SourceGroupConfig::SOURCE_GROUP_CONFIG_PARAMETER = "parameter";

const string SourceGroupConfig::FIELD_MODE_UNKNOWN = "unknown";
const string SourceGroupConfig::FIELD_MODE_USER_DEFINE = "user_define";
const string SourceGroupConfig::FIELD_MODE_ALL_FIELD = "all_field";
const string SourceGroupConfig::FIELD_MODE_SPECIFIED_FIELD = "specified_field";

struct SourceGroupConfig::Impl {
    SourceGroupConfig::SourceFieldMode fieldMode;
    std::vector<std::string> fields;
    GroupDataParameter parameter;
    index::sourcegroupid_t id = index::INVALID_SOURCEGROUPID;
    bool disabled = false;
};

SourceGroupConfig::SourceGroupConfig() : _impl(std::make_unique<Impl>()) {}

SourceGroupConfig::~SourceGroupConfig() {}

SourceGroupConfig::SourceGroupConfig(const SourceGroupConfig& other) : _impl(std::make_unique<Impl>(*other._impl)) {}

void SourceGroupConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        string modeStr = ToSourceFieldModeStr(_impl->fieldMode);
        json.Jsonize(SOURCE_GROUP_CONFIG_FIELD_MODE, modeStr);
    } else {
        string modeStr;
        json.Jsonize(SOURCE_GROUP_CONFIG_FIELD_MODE, modeStr);
        _impl->fieldMode = FromSourceFieldModeStr(modeStr);
    }
    json.Jsonize(SOURCE_GROUP_CONFIG_FIELDS, _impl->fields, _impl->fields);
    json.Jsonize(SOURCE_GROUP_CONFIG_PARAMETER, _impl->parameter, _impl->parameter);
}

Status SourceGroupConfig::CheckEqual(const SourceGroupConfig& other) const
{
    CHECK_CONFIG_EQUAL(_impl->fieldMode, other._impl->fieldMode, "FieldMode not equal");
    CHECK_CONFIG_EQUAL(_impl->fields, other._impl->fields, "Fields not equal");
    CHECK_CONFIG_EQUAL(_impl->id, other._impl->id, "groupId not equal");
    CHECK_CONFIG_EQUAL(_impl->disabled, other._impl->disabled, "disable flag not equal");
    return _impl->parameter.CheckEqual(other._impl->parameter);
}

Status SourceGroupConfig::Check() const
{
    if (_impl->fieldMode == SourceGroupConfig::SourceFieldMode::SPECIFIED_FIELD && _impl->fields.empty()) {
        RETURN_STATUS_ERROR(ConfigError, "source_group_config error: must config no-empty fields when field_mode=%s",
                            SourceGroupConfig::ToSourceFieldModeStr(_impl->fieldMode).c_str());
    }

    return Status::OK();
}

SourceGroupConfig::SourceFieldMode SourceGroupConfig::GetFieldMode() const { return _impl->fieldMode; }

void SourceGroupConfig::SetFieldMode(SourceGroupConfig::SourceFieldMode mode) { _impl->fieldMode = mode; }

SourceGroupConfig::SourceFieldMode SourceGroupConfig::FromSourceFieldModeStr(const string& modeStr)
{
    if (modeStr == FIELD_MODE_USER_DEFINE) {
        return SourceGroupConfig::SourceFieldMode::USER_DEFINE;
    } else if (modeStr == FIELD_MODE_ALL_FIELD) {
        return SourceGroupConfig::SourceFieldMode::ALL_FIELD;
    } else if (modeStr == FIELD_MODE_SPECIFIED_FIELD) {
        return SourceGroupConfig::SourceFieldMode::SPECIFIED_FIELD;
    } else {
        return SourceGroupConfig::SourceFieldMode::UNKNOWN;
    }
}

string SourceGroupConfig::ToSourceFieldModeStr(SourceFieldMode mode)
{
    switch (mode) {
    case SourceGroupConfig::SourceFieldMode::USER_DEFINE:
        return FIELD_MODE_USER_DEFINE;
    case SourceGroupConfig::SourceFieldMode::ALL_FIELD:
        return FIELD_MODE_ALL_FIELD;
    case SourceGroupConfig::SourceFieldMode::SPECIFIED_FIELD:
        return FIELD_MODE_SPECIFIED_FIELD;
    default:
        return FIELD_MODE_UNKNOWN;
    }
}

const vector<string>& SourceGroupConfig::GetSpecifiedFields() const { return _impl->fields; }

void SourceGroupConfig::SetSpecifiedFields(const vector<string>& fields) { _impl->fields = fields; }

index::sourcegroupid_t SourceGroupConfig::GetGroupId() const { return _impl->id; }

void SourceGroupConfig::SetGroupId(index::sourcegroupid_t groupId) { _impl->id = groupId; }

const GroupDataParameter& SourceGroupConfig::GetParameter() const { return _impl->parameter; }

void SourceGroupConfig::SetParameter(const GroupDataParameter& parameter) { _impl->parameter = parameter; }

bool SourceGroupConfig::IsDisabled() const { return _impl->disabled; }

void SourceGroupConfig::SetDisabled(bool flag) { _impl->disabled = flag; }

}} // namespace indexlib::config
