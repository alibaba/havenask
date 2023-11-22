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
#include "indexlib/config/schema_modify_operation.h"

#include "indexlib/config/impl/schema_modify_operation_impl.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, SchemaModifyOperation);

SchemaModifyOperation::SchemaModifyOperation() { mImpl.reset(new SchemaModifyOperationImpl); }

SchemaModifyOperation::~SchemaModifyOperation() {}

void SchemaModifyOperation::Jsonize(Jsonizable::JsonWrapper& json) { mImpl->Jsonize(json); }

schema_opid_t SchemaModifyOperation::GetOpId() const { return mImpl->GetOpId(); }

void SchemaModifyOperation::SetOpId(schema_opid_t id) { mImpl->SetOpId(id); }

void SchemaModifyOperation::LoadDeleteOperation(const Any& any, IndexPartitionSchemaImpl& schema)
{
    mImpl->LoadDeleteOperation(any, schema);
}

void SchemaModifyOperation::LoadAddOperation(const Any& any, IndexPartitionSchemaImpl& schema)
{
    mImpl->LoadAddOperation(any, schema);
}

void SchemaModifyOperation::MarkNotReady() { mImpl->MarkNotReady(); }

bool SchemaModifyOperation::IsNotReady() const { return mImpl->IsNotReady(); }

void SchemaModifyOperation::CollectEffectiveModifyItem(ModifyItemVector& indexs, ModifyItemVector& attrs)
{
    return mImpl->CollectEffectiveModifyItem(indexs, attrs);
}

void SchemaModifyOperation::AssertEqual(const SchemaModifyOperation& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}

const map<string, string>& SchemaModifyOperation::GetParams() const { return mImpl->GetParams(); }

void SchemaModifyOperation::SetParams(const map<string, string>& params) { mImpl->SetParams(params); }

void SchemaModifyOperation::Validate() const { mImpl->Validate(); }

const vector<FieldConfigPtr>& SchemaModifyOperation::GetAddFields() const { return mImpl->GetAddFields(); }

const vector<IndexConfigPtr>& SchemaModifyOperation::GetAddIndexs() const { return mImpl->GetAddIndexs(); }

const vector<AttributeConfigPtr>& SchemaModifyOperation::GetAddAttributes() const { return mImpl->GetAddAttributes(); }

const vector<string>& SchemaModifyOperation::GetDeleteFields() const { return mImpl->GetDeleteFields(); }

const vector<string>& SchemaModifyOperation::GetDeleteIndexs() const { return mImpl->GetDeleteIndexs(); }

const vector<string>& SchemaModifyOperation::GetDeleteAttrs() const { return mImpl->GetDeleteAttrs(); }
}} // namespace indexlib::config
