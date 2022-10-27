#include "indexlib/config/field_schema.h"
#include "indexlib/config/impl/field_schema_impl.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace std::tr1;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, FieldSchema);

FieldSchema::FieldSchema(size_t capacity)
{
    mImpl.reset(new FieldSchemaImpl(capacity));
}

void FieldSchema::AddFieldConfig(const FieldConfigPtr& fieldConfig)
{
    mImpl->AddFieldConfig(fieldConfig);
}

FieldConfigPtr FieldSchema::GetFieldConfig(fieldid_t fieldId) const
{
    return mImpl->GetFieldConfig(fieldId);
}

FieldConfigPtr FieldSchema::GetFieldConfig(const string& fieldName) const
{
    return mImpl->GetFieldConfig(fieldName);
}

fieldid_t FieldSchema::GetFieldId(const string& fieldName) const
{
    return mImpl->GetFieldId(fieldName);
}

size_t FieldSchema::GetFieldCount() const
{
    return mImpl->GetFieldCount();
}

FieldSchema::Iterator FieldSchema::Begin() const
{
    return mImpl->Begin();
}

FieldSchema::Iterator FieldSchema::End() const
{
    return mImpl->End();
}

void FieldSchema::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}

void FieldSchema::AssertEqual(const FieldSchema& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}

void FieldSchema::AssertCompatible(const FieldSchema& other) const
{
    mImpl->AssertCompatible(*(other.mImpl.get()));
}

bool FieldSchema::IsFieldNameInSchema(const string& fieldName) const
{
    return mImpl->IsFieldNameInSchema(fieldName);
}

bool FieldSchema::IsFieldTypeSortable(const string& fieldName, 
                         const std::string& fieldType) const
{
    return mImpl->IsFieldTypeSortable(fieldName, fieldType);
}

void FieldSchema::DeleteField(const std::string& field)
{
    mImpl->DeleteField(field);
}

void FieldSchema::SetBaseSchemaImmutable()
{
    mImpl->SetBaseSchemaImmutable();
}

void FieldSchema::SetModifySchemaImmutable()
{
    mImpl->SetModifySchemaImmutable();
}

void FieldSchema::SetModifySchemaMutable()
{
    mImpl->SetModifySchemaMutable();
}

FieldConfigIteratorPtr FieldSchema::CreateIterator(
        ConfigIteratorType type) const
{
    return mImpl->CreateIterator(type);
}

bool FieldSchema::IsDeleted(fieldid_t fieldId) const
{
    return mImpl->IsDeleted(fieldId);
}

void FieldSchema::RewriteFieldType()
{
    mImpl->RewriteFieldType();
}

IE_NAMESPACE_END(config);

