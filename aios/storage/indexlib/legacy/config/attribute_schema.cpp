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
#include "indexlib/config/attribute_schema.h"

#include "indexlib/config/impl/attribute_schema_impl.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, AttributeSchema);

AttributeSchema::AttributeSchema() { mImpl.reset(new AttributeSchemaImpl()); }

const AttributeConfigPtr& AttributeSchema::GetAttributeConfig(const string& attrName) const
{
    return mImpl->GetAttributeConfig(attrName);
}

const AttributeConfigPtr& AttributeSchema::GetAttributeConfig(attrid_t attrId) const
{
    return mImpl->GetAttributeConfig(attrId);
}

const AttributeConfigPtr& AttributeSchema::GetAttributeConfigByFieldId(fieldid_t fieldId) const
{
    return mImpl->GetAttributeConfigByFieldId(fieldId);
}

const AttributeSchema::AttrVector& AttributeSchema::GetAttributeConfigs() const { return mImpl->GetAttributeConfigs(); }

const PackAttributeConfigPtr& AttributeSchema::GetPackAttributeConfig(const string& packAttrName) const
{
    return mImpl->GetPackAttributeConfig(packAttrName);
}

const PackAttributeConfigPtr& AttributeSchema::GetPackAttributeConfig(packattrid_t packId) const
{
    return mImpl->GetPackAttributeConfig(packId);
}

size_t AttributeSchema::GetPackAttributeCount() const { return mImpl->GetPackAttributeCount(); }

packattrid_t AttributeSchema::GetPackIdByAttributeId(attrid_t attrId) const
{
    return mImpl->GetPackIdByAttributeId(attrId);
}

void AttributeSchema::AddAttributeConfig(const AttributeConfigPtr& attrConfig)
{
    mImpl->AddAttributeConfig(attrConfig);
}
void AttributeSchema::AddPackAttributeConfig(const PackAttributeConfigPtr& packAttrConfig)
{
    mImpl->AddPackAttributeConfig(packAttrConfig);
}

size_t AttributeSchema::GetAttributeCount() const { return mImpl->GetAttributeCount(); }

AttributeSchema::Iterator AttributeSchema::Begin() const { return mImpl->Begin(); }
AttributeSchema::Iterator AttributeSchema::End() const { return mImpl->End(); }

bool AttributeSchema::IsInAttribute(fieldid_t fieldId) const { return mImpl->IsInAttribute(fieldId); }
bool AttributeSchema::IsInAttribute(const string& attrName) const { return mImpl->IsInAttribute(attrName); }
void AttributeSchema::SetExistAttrConfig(fieldid_t fieldId, const AttributeConfigPtr& attrConf)
{
    mImpl->SetExistAttrConfig(fieldId, attrConf);
}

void AttributeSchema::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) { mImpl->Jsonize(json); }
void AttributeSchema::AssertEqual(const AttributeSchema& other) const { mImpl->AssertEqual(*(other.mImpl.get())); }
void AttributeSchema::AssertCompatible(const AttributeSchema& other) const
{
    mImpl->AssertCompatible(*(other.mImpl.get()));
}
bool AttributeSchema::HasSameAttributeConfigs(AttributeSchemaPtr& other) const
{
    if (!other) {
        return false;
    }
    return mImpl->HasSameAttributeConfigs(other->GetImpl());
}
bool AttributeSchema::DisableAttribute(const string& attrName) { return mImpl->DisableAttribute(attrName); }
bool AttributeSchema::DisablePackAttribute(const string& packAttrName)
{
    return mImpl->DisablePackAttribute(packAttrName);
}

fieldid_t AttributeSchema::GetMaxFieldId() const { return mImpl->GetMaxFieldId(); }

const AttributeSchemaImplPtr& AttributeSchema::GetImpl() { return mImpl; }

void AttributeSchema::DeleteAttribute(const string& attrName) { mImpl->DeleteAttribute(attrName); }

void AttributeSchema::SetBaseSchemaImmutable() { mImpl->SetBaseSchemaImmutable(); }

void AttributeSchema::SetModifySchemaImmutable() { mImpl->SetModifySchemaImmutable(); }

void AttributeSchema::SetModifySchemaMutable() { mImpl->SetModifySchemaMutable(); }

AttributeConfigIteratorPtr AttributeSchema::CreateIterator(IndexStatus type) const
{
    return mImpl->CreateIterator(type);
}

PackAttributeConfigIteratorPtr AttributeSchema::CreatePackAttrIterator(IndexStatus type) const
{
    return mImpl->CreatePackAttrIterator(type);
}

bool AttributeSchema::IsDeleted(attrid_t attrId) const { return mImpl->IsDeleted(attrId); }

void AttributeSchema::CollectBaseVersionAttrInfo(ModifyItemVector& attrs) const
{
    return mImpl->CollectBaseVersionAttrInfo(attrs);
}
}} // namespace indexlib::config
