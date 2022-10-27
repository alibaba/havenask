#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/impl/attribute_schema_impl.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, AttributeSchema);

AttributeSchema::AttributeSchema()
{
    mImpl.reset(new AttributeSchemaImpl());
}

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

const AttributeSchema::AttrVector& AttributeSchema::GetAttributeConfigs() const
{
    return mImpl->GetAttributeConfigs();
}

const PackAttributeConfigPtr& AttributeSchema::GetPackAttributeConfig(const string& packAttrName) const
{
    return mImpl->GetPackAttributeConfig(packAttrName);
}

const PackAttributeConfigPtr& AttributeSchema::GetPackAttributeConfig(packattrid_t packId) const
{
    return mImpl->GetPackAttributeConfig(packId);
}

size_t AttributeSchema::GetPackAttributeCount() const
{
    return mImpl->GetPackAttributeCount();
}

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

size_t AttributeSchema::GetAttributeCount() const 
{
    return mImpl->GetAttributeCount();
}

AttributeSchema::Iterator AttributeSchema::Begin() const {
    return mImpl->Begin();
}
AttributeSchema::Iterator AttributeSchema::End() const {
    return mImpl->End();
}

bool AttributeSchema::IsInAttribute(fieldid_t fieldId) const
{
    return mImpl->IsInAttribute(fieldId);
}
bool AttributeSchema::IsInAttribute(const string &attrName) const
{
    return mImpl->IsInAttribute(attrName);
}
void AttributeSchema::SetExistAttrConfig(fieldid_t fieldId, const AttributeConfigPtr& attrConf)
{
    mImpl->SetExistAttrConfig(fieldId, attrConf);
}

void AttributeSchema::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}
void AttributeSchema::AssertEqual(const AttributeSchema& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}
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
bool AttributeSchema::DisableAttribute(const string& attrName)
{
    return mImpl->DisableAttribute(attrName);
}
bool AttributeSchema::DisablePackAttribute(const string& packAttrName)
{
    return mImpl->DisablePackAttribute(packAttrName);
}

fieldid_t AttributeSchema::GetMaxFieldId() const 
{
    return mImpl->GetMaxFieldId();
}

const AttributeSchemaImplPtr& AttributeSchema::GetImpl()
{
    return mImpl;
}

void AttributeSchema::DeleteAttribute(const string& attrName)
{
    mImpl->DeleteAttribute(attrName);
}

void AttributeSchema::SetBaseSchemaImmutable()
{
    mImpl->SetBaseSchemaImmutable();
}

void AttributeSchema::SetModifySchemaImmutable()
{
    mImpl->SetModifySchemaImmutable();
}

void AttributeSchema::SetModifySchemaMutable()
{
    mImpl->SetModifySchemaMutable();
}

AttributeConfigIteratorPtr AttributeSchema::CreateIterator(ConfigIteratorType type) const
{
    return mImpl->CreateIterator(type);
}

PackAttributeConfigIteratorPtr AttributeSchema::CreatePackAttrIterator(ConfigIteratorType type) const
{
    return mImpl->CreatePackAttrIterator(type);
}

bool AttributeSchema::IsDeleted(attrid_t attrId) const
{
    return mImpl->IsDeleted(attrId);
}

void AttributeSchema::CollectBaseVersionAttrInfo(ModifyItemVector& attrs) const
{
    return mImpl->CollectBaseVersionAttrInfo(attrs);
}

IE_NAMESPACE_END(config);

