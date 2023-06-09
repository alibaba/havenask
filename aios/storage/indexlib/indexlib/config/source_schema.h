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
#ifndef __INDEXLIB_SOURCE_SCHEMA_H
#define __INDEXLIB_SOURCE_SCHEMA_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/config/module_info.h"
#include "indexlib/config/source_group_config.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, SourceSchemaImpl);

namespace indexlib { namespace config {

class SourceSchema : public autil::legacy::Jsonizable
{
public:
    typedef SourceGroupConfigVector::const_iterator Iterator;

public:
    typedef std::vector<std::string> FieldGroup;
    typedef std::vector<FieldGroup> FieldGroups;

public:
    SourceSchema();
    ~SourceSchema();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const SourceSchema& other) const;
    void Check() const;

    void DisableAllFields();
    bool IsAllFieldsDisabled() const;
    void DisableFieldGroup(index::groupid_t groupId);

    void GetDisableGroupIds(std::vector<index::groupid_t>& ids) const;

public:
    size_t GetSourceGroupCount() const;
    Iterator Begin() const;
    Iterator End() const;
    bool GetModule(const std::string& moduleName, ModuleInfo& ret) const;
    void AddGroupConfig(const SourceGroupConfigPtr& groupConfig);
    const SourceGroupConfigPtr& GetGroupConfig(index::groupid_t groupId) const;

private:
    SourceSchemaImplPtr mImpl;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SourceSchema);
}} // namespace indexlib::config

#endif //__INDEXLIB_SOURCE_SCHEMA_H
