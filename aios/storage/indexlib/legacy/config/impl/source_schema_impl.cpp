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
#include "indexlib/config/impl/source_schema_impl.h"

#include "indexlib/config/config_define.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, SourceSchemaImpl);

SourceSchemaImpl::SourceSchemaImpl() : mIsAllFieldsDisabled(false) {}

SourceSchemaImpl::~SourceSchemaImpl() {}

void SourceSchemaImpl::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize(SOURCE_GROUP_CONFIGS, mGroupConfigs, mGroupConfigs);
    if (json.GetMode() == FROM_JSON) {
        for (size_t gid = 0; gid < mGroupConfigs.size(); gid++) {
            mGroupConfigs[gid]->SetGroupId(gid);
        }
    }
}

void SourceSchemaImpl::AssertEqual(const SourceSchemaImpl& other) const
{
    for (size_t i = 0; i < mGroupConfigs.size(); i++) {
        auto status = mGroupConfigs[i]->CheckEqual(*(other.mGroupConfigs[i].get()));
        if (!status.IsOK()) {
            AUTIL_LEGACY_THROW(util::AssertEqualException, "group config not equal");
        }
    }
    IE_CONFIG_ASSERT_EQUAL(mIsAllFieldsDisabled, other.mIsAllFieldsDisabled, "disable fields not equal");
}

void SourceSchemaImpl::Check() const
{
    if (mGroupConfigs.empty()) {
        INDEXLIB_FATAL_ERROR(Schema, "SourceSchema should have at least one group config.");
    }
}

void SourceSchemaImpl::AddGroupConfig(const SourceGroupConfigPtr& groupConfig) { mGroupConfigs.push_back(groupConfig); }

const SourceGroupConfigPtr& SourceSchemaImpl::GetGroupConfig(index::sourcegroupid_t groupId) const
{
    if (groupId < 0 || groupId >= (index::sourcegroupid_t)mGroupConfigs.size()) {
        static SourceGroupConfigPtr emptyConfig;
        return emptyConfig;
    }
    return mGroupConfigs[groupId];
}

void SourceSchemaImpl::DisableAllFields()
{
    mIsAllFieldsDisabled = true;
    for (auto group : mGroupConfigs) {
        group->SetDisabled(true);
    }
}

bool SourceSchemaImpl::IsAllFieldsDisabled() const { return mIsAllFieldsDisabled; }

size_t SourceSchemaImpl::GetSourceGroupCount() const { return mGroupConfigs.size(); }

void SourceSchemaImpl::DisableFieldGroup(index::sourcegroupid_t groupId)
{
    if (groupId < 0 || groupId >= mGroupConfigs.size()) {
        AUTIL_LOG(WARN, "group id [%d] for source index not exist.", groupId);
        return;
    }

    mGroupConfigs[groupId]->SetDisabled(true);
    for (auto groupConfig : mGroupConfigs) {
        if (!groupConfig->IsDisabled()) {
            mIsAllFieldsDisabled = false;
            return;
        }
    }
    mIsAllFieldsDisabled = true;
}

void SourceSchemaImpl::GetDisableGroupIds(std::vector<index::sourcegroupid_t>& ids) const
{
    assert(ids.empty());
    for (auto groupConfig : mGroupConfigs) {
        if (groupConfig->IsDisabled()) {
            ids.push_back(groupConfig->GetGroupId());
        }
    }
}
}} // namespace indexlib::config
