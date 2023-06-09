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
#ifndef __INDEXLIB_SOURCE_SCHEMA_IMPL_H
#define __INDEXLIB_SOURCE_SCHEMA_IMPL_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/config/module_info.h"
#include "indexlib/config/source_group_config.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace config {

class SourceSchemaImpl : public autil::legacy::Jsonizable
{
public:
    typedef SourceGroupConfigVector::const_iterator Iterator;

public:
    SourceSchemaImpl();
    ~SourceSchemaImpl();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const SourceSchemaImpl& other) const;
    void Check() const;

    void DisableAllFields();
    bool IsAllFieldsDisabled() const;
    void DisableFieldGroup(index::groupid_t groupId);

    size_t GetSourceGroupCount() const;
    Iterator Begin() const { return mGroupConfigs.begin(); }
    Iterator End() const { return mGroupConfigs.end(); }
    bool GetModule(const std::string& moduleName, ModuleInfo& ret) const;
    void AddGroupConfig(const SourceGroupConfigPtr& groupConfig);
    const SourceGroupConfigPtr& GetGroupConfig(index::groupid_t groupId) const;

    void GetDisableGroupIds(std::vector<index::groupid_t>& ids) const;

private:
    bool IsModuleExist(const std::string& moduleName) const;

private:
    SourceGroupConfigVector mGroupConfigs;
    ModuleInfos mModules;
    bool mIsAllFieldsDisabled;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SourceSchemaImpl);
}} // namespace indexlib::config

#endif //__INDEXLIB_SOURCE_SCHEMA_IMPL_H
