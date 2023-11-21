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
#include "indexlib/config/index_schema.h"

#include "indexlib/config/impl/index_schema_impl.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {

AUTIL_LOG_SETUP(indexlib.config, IndexSchema);

IndexSchema::IndexSchema() { mImpl.reset(new IndexSchemaImpl()); }

void IndexSchema::AddIndexConfig(const IndexConfigPtr& indexInfo) { mImpl->AddIndexConfig(indexInfo); }
IndexConfigPtr IndexSchema::GetIndexConfig(const string& indexName) const { return mImpl->GetIndexConfig(indexName); }
IndexConfigPtr IndexSchema::GetIndexConfig(indexid_t id) const { return mImpl->GetIndexConfig(id); }

const vector<indexid_t>& IndexSchema::GetIndexIdList(fieldid_t fieldId) const { return mImpl->GetIndexIdList(fieldId); }
size_t IndexSchema::GetIndexCount() const { return mImpl->GetIndexCount(); }

IndexSchema::Iterator IndexSchema::Begin() const { return mImpl->Begin(); }
IndexSchema::Iterator IndexSchema::End() const { return mImpl->End(); }

bool IndexSchema::IsInIndex(fieldid_t fieldId) const { return mImpl->IsInIndex(fieldId); }
void IndexSchema::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) { mImpl->Jsonize(json); }
void IndexSchema::AssertEqual(const IndexSchema& other) const { mImpl->AssertEqual(*(other.mImpl.get())); }
void IndexSchema::AssertCompatible(const IndexSchema& other) const { mImpl->AssertCompatible(*(other.mImpl.get())); }
bool IndexSchema::AllIndexUpdatableForField(fieldid_t fieldId) const
{
    return mImpl->AllIndexUpdatableForField(fieldId);
}
bool IndexSchema::AnyIndexUpdatable() const { return mImpl->AnyIndexUpdatable(); }

// API for primary key start
bool IndexSchema::HasPrimaryKeyIndex() const { return mImpl->HasPrimaryKeyIndex(); }
bool IndexSchema::HasPrimaryKeyAttribute() const { return mImpl->HasPrimaryKeyAttribute(); }
InvertedIndexType IndexSchema::GetPrimaryKeyIndexType() const { return mImpl->GetPrimaryKeyIndexType(); }
string IndexSchema::GetPrimaryKeyIndexFieldName() const { return mImpl->GetPrimaryKeyIndexFieldName(); }
fieldid_t IndexSchema::GetPrimaryKeyIndexFieldId() const { return mImpl->GetPrimaryKeyIndexFieldId(); }
const SingleFieldIndexConfigPtr& IndexSchema::GetPrimaryKeyIndexConfig() const
{
    return mImpl->GetPrimaryKeyIndexConfig();
}
// API for primary key end

void IndexSchema::LoadTruncateTermInfo(const file_system::DirectoryPtr& metaDir)
{
    mImpl->LoadTruncateTermInfo(metaDir);
}

void IndexSchema::Check() { mImpl->Check(); }

void IndexSchema::SetPrimaryKeyIndexConfig(const SingleFieldIndexConfigPtr& config)
{
    mImpl->SetPrimaryKeyIndexConfig(config);
}

void IndexSchema::DeleteIndex(const string& indexName) { mImpl->DeleteIndex(indexName); }

void IndexSchema::DisableIndex(const string& indexName) { mImpl->DisableIndex(indexName); }

void IndexSchema::EnableBloomFilterForIndex(const string& indexName, uint32_t multipleNum)
{
    mImpl->EnableBloomFilterForIndex(indexName, multipleNum);
}

void IndexSchema::SetBaseSchemaImmutable() { mImpl->SetBaseSchemaImmutable(); }

void IndexSchema::SetModifySchemaImmutable() { mImpl->SetModifySchemaImmutable(); }

void IndexSchema::SetModifySchemaMutable() { mImpl->SetModifySchemaMutable(); }

IndexConfigIteratorPtr IndexSchema::CreateIterator(bool needVirtual, IndexStatus type) const
{
    return mImpl->CreateIterator(needVirtual, type);
}

bool IndexSchema::IsDeleted(indexid_t id) const { return mImpl->IsDeleted(id); }

void IndexSchema::CollectBaseVersionIndexInfo(ModifyItemVector& indexs) const
{
    return mImpl->CollectBaseVersionIndexInfo(indexs);
}
}} // namespace indexlib::config
