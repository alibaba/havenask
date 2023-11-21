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
#include "indexlib/config/source_group_config.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib::config {
class SourceSchemaImpl;
typedef std::shared_ptr<SourceSchemaImpl> SourceSchemaImplPtr;
} // namespace indexlib::config

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
    void DisableFieldGroup(index::sourcegroupid_t groupId);

    void GetDisableGroupIds(std::vector<index::sourcegroupid_t>& ids) const;

public:
    size_t GetSourceGroupCount() const;
    Iterator Begin() const;
    Iterator End() const;
    void AddGroupConfig(const SourceGroupConfigPtr& groupConfig);
    const SourceGroupConfigPtr& GetGroupConfig(index::sourcegroupid_t groupId) const;

private:
    SourceSchemaImplPtr mImpl;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SourceSchema> SourceSchemaPtr;
}} // namespace indexlib::config
