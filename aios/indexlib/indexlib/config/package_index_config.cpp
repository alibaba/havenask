#include "indexlib/config/package_index_config.h"
#include "indexlib/config/impl/package_index_config_impl.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, PackageIndexConfig);

const uint32_t PackageIndexConfig::PACK_MAX_FIELD_NUM = 32;
const uint32_t PackageIndexConfig::EXPACK_MAX_FIELD_NUM = 8;
const uint32_t PackageIndexConfig::CUSTOMIZED_MAX_FIELD_NUM = 32;

PackageIndexConfig::PackageIndexConfig(
        const string& indexName, IndexType indexType)
    : IndexConfig(indexName, indexType)
    , mImpl(new PackageIndexConfigImpl(indexName, indexType))
{
    IndexConfig::ResetImpl(mImpl);
}

PackageIndexConfig::PackageIndexConfig(const PackageIndexConfig& other)
    : IndexConfig(other)
    , mImpl(new PackageIndexConfigImpl(*(other.mImpl)))
{
    IndexConfig::ResetImpl(mImpl);
}

PackageIndexConfig::~PackageIndexConfig() 
{ }

uint32_t PackageIndexConfig::GetFieldCount() const
{
    return mImpl->GetFieldCount();
}
IndexConfig::Iterator PackageIndexConfig::CreateIterator() const 
{
    return mImpl->CreateIterator();
}
    
int32_t PackageIndexConfig::GetFieldBoost(fieldid_t id) const 
{
    return mImpl->GetFieldBoost(id);
}
void PackageIndexConfig::SetFieldBoost(fieldid_t id, int32_t boost)
{
    mImpl->SetFieldBoost(id, boost);
}

int32_t PackageIndexConfig::GetFieldIdxInPack(fieldid_t id) const
{
    return mImpl->GetFieldIdxInPack(id);
}

int32_t PackageIndexConfig::GetFieldIdxInPack(const std::string& fieldName) const
{
    return mImpl->GetFieldIdxInPack(fieldName);
}

fieldid_t PackageIndexConfig::GetFieldId(int32_t fieldIdxInPack) const
{
    return mImpl->GetFieldId(fieldIdxInPack);
}

int32_t PackageIndexConfig::GetTotalFieldBoost() const
{
    return mImpl->GetTotalFieldBoost();
}

void PackageIndexConfig::AddFieldConfig(const FieldConfigPtr& fieldConfig, int32_t boost)
{
    mImpl->AddFieldConfig(fieldConfig, boost);
}
void PackageIndexConfig::AddFieldConfig(fieldid_t id,int32_t boost)
{
    mImpl->AddFieldConfig(id, boost);
}
void PackageIndexConfig::AddFieldConfig(const std::string& fieldName, int32_t boost)
{
    mImpl->AddFieldConfig(fieldName, boost);
}

bool PackageIndexConfig::HasSectionAttribute() const
{
    return mImpl->HasSectionAttribute();
}

const SectionAttributeConfigPtr &PackageIndexConfig::GetSectionAttributeConfig() const
{
    return mImpl->GetSectionAttributeConfig();
}

void PackageIndexConfig::SetHasSectionAttributeFlag(bool flag)
{
    mImpl->SetHasSectionAttributeFlag(flag);
}

const FieldConfigVector& PackageIndexConfig::GetFieldConfigVector() const
{
    return mImpl->GetFieldConfigVector();
}

void PackageIndexConfig::SetFieldSchema(const FieldSchemaPtr& fieldSchema)
{
    mImpl->SetFieldSchema(fieldSchema);
}

bool PackageIndexConfig::IsInIndex(fieldid_t fieldId) const
{
    return mImpl->IsInIndex(fieldId);
}

void PackageIndexConfig::SetMaxFirstOccInDoc(pos_t pos)
{
    mImpl->SetMaxFirstOccInDoc(pos);
}

pos_t PackageIndexConfig::GetMaxFirstOccInDoc() const 
{
    return mImpl->GetMaxFirstOccInDoc();
}
    
void PackageIndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}
void PackageIndexConfig::AssertEqual(const IndexConfig& other) const
{
    const PackageIndexConfig& other2 = (const PackageIndexConfig&)other;
    mImpl->AssertEqual(*(other2.mImpl));
}
void PackageIndexConfig::AssertCompatible(const IndexConfig& other) const
{
    const PackageIndexConfig& other2 = (const PackageIndexConfig&)other;
    mImpl->AssertCompatible(*(other2.mImpl));
}
IndexConfig* PackageIndexConfig::Clone() const
{
    return new PackageIndexConfig(*this);
}
void PackageIndexConfig::SetDefaultAnalyzer()
{
    mImpl->SetDefaultAnalyzer();
}

//only for test
void PackageIndexConfig::SetSectionAttributeConfig(SectionAttributeConfigPtr sectionAttributeConfig)
{
    mImpl->SetSectionAttributeConfig(sectionAttributeConfig);
}

bool PackageIndexConfig::CheckFieldType(FieldType ft) const
{
    return mImpl->CheckFieldType(ft);
}

void PackageIndexConfig::ResetImpl(IndexConfigImpl* impl)
{
    IndexConfig::ResetImpl(impl);
    mImpl = (PackageIndexConfigImpl*)impl;
}

IE_NAMESPACE_END(config);

