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

#include <memory>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/source/Types.h"

namespace indexlib { namespace config {
class GroupDataParameter;

class SourceGroupConfig : public autil::legacy::Jsonizable
{
public:
    const static std::string FIELD_MODE_UNKNOWN;
    const static std::string FIELD_MODE_USER_DEFINE;
    const static std::string FIELD_MODE_ALL_FIELD;
    const static std::string FIELD_MODE_SPECIFIED_FIELD;
    enum SourceFieldMode { UNKNOWN, USER_DEFINE, ALL_FIELD, SPECIFIED_FIELD };

    static const std::string SOURCE_GROUP_CONFIG_FIELD_MODE;
    static const std::string SOURCE_GROUP_CONFIG_FIELDS;
    static const std::string SOURCE_GROUP_CONFIG_PARAMETER;
    static const std::string SOURCE_GROUP_CONFIG_MODULE;
    static const std::string SOURCE_GROUP_CONFIG_MODULE_PARAMS;

public:
    SourceGroupConfig();
    ~SourceGroupConfig();
    SourceGroupConfig(const SourceGroupConfig&);

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    Status CheckEqual(const SourceGroupConfig& other) const;
    Status Check() const;
    const std::string& GetModule() const;
    const std::vector<std::string>& GetSpecifiedFields() const;
    SourceFieldMode GetFieldMode() const;
    index::groupid_t GetGroupId() const;
    const GroupDataParameter& GetParameter() const;

    void SetFieldMode(SourceFieldMode mode);
    void SetSpecifiedFields(const std::vector<std::string>& fields);
    void SetGroupId(index::groupid_t groupId);
    void SetParameter(const GroupDataParameter& parameter);

    bool IsDisabled() const;
    void SetDisabled(bool flag);

public:
    static std::string ToSourceFieldModeStr(SourceFieldMode mode);
    static SourceFieldMode FromSourceFieldModeStr(const std::string& modeStr);

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::vector<std::shared_ptr<SourceGroupConfig>> SourceGroupConfigVector;

}} // namespace indexlib::config
