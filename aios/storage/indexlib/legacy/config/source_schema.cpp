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
#include "indexlib/config/source_schema.h"

#include "indexlib/config/impl/source_schema_impl.h"

using namespace std;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, SourceSchema);

SourceSchema::SourceSchema() { mImpl.reset(new SourceSchemaImpl()); }

SourceSchema::~SourceSchema() {}

void SourceSchema::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) { mImpl->Jsonize(json); }

void SourceSchema::AssertEqual(const SourceSchema& other) const { mImpl->AssertEqual(*(other.mImpl.get())); }

void SourceSchema::Check() const { mImpl->Check(); }

size_t SourceSchema::GetSourceGroupCount() const { return mImpl->GetSourceGroupCount(); }

SourceSchema::Iterator SourceSchema::Begin() const { return mImpl->Begin(); }

SourceSchema::Iterator SourceSchema::End() const { return mImpl->End(); }

void SourceSchema::AddGroupConfig(const SourceGroupConfigPtr& groupConfig) { mImpl->AddGroupConfig(groupConfig); }

const SourceGroupConfigPtr& SourceSchema::GetGroupConfig(index::sourcegroupid_t groupId) const
{
    return mImpl->GetGroupConfig(groupId);
}

void SourceSchema::DisableAllFields() { mImpl->DisableAllFields(); }

bool SourceSchema::IsAllFieldsDisabled() const { return mImpl->IsAllFieldsDisabled(); }

void SourceSchema::DisableFieldGroup(index::sourcegroupid_t groupId) { mImpl->DisableFieldGroup(groupId); }

void SourceSchema::GetDisableGroupIds(std::vector<index::sourcegroupid_t>& ids) const
{
    mImpl->GetDisableGroupIds(ids);
}
}} // namespace indexlib::config
