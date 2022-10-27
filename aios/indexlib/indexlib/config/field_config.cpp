#include "indexlib/config/field_config.h"
#include "indexlib/config/impl/field_config_impl.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);

IE_LOG_SETUP(config, FieldConfig);

FieldConfig::FieldConfig()
{
    mImpl.reset(new FieldConfigImpl());
}

FieldConfig::FieldConfig(const string& fieldName, 
                         FieldType fieldType, bool multiValue)
{
    mImpl.reset(new FieldConfigImpl(fieldName, fieldType, multiValue));
}

void FieldConfig::ResetImpl(FieldConfigImpl* impl)
{
    mImpl.reset(impl);
}

FieldConfig::FieldConfig(const string& fieldName,FieldType fieldType, 
                         bool multiValue, bool isVirtual, bool isBinary)
{
    mImpl.reset(new FieldConfigImpl(fieldName, fieldType, multiValue,
                                    isVirtual, isBinary));
}

fieldid_t FieldConfig::GetFieldId() const
{
    return mImpl->GetFieldId();
}

void FieldConfig::SetFieldId(fieldid_t id)
{
    mImpl->SetFieldId(id);
}

const string& FieldConfig::GetFieldName() const
{
    return mImpl->GetFieldName();
}

void FieldConfig::SetFieldName(const string& fieldName)
{
    mImpl->SetFieldName(fieldName);
}

const string& FieldConfig::GetAnalyzerName() const
{
    return mImpl->GetAnalyzerName();
}

void FieldConfig::SetAnalyzerName(const string& analyzerName)
{
    mImpl->SetAnalyzerName(analyzerName);
} 

FieldType FieldConfig::GetFieldType() const
{
    return mImpl->GetFieldType();
}
void FieldConfig::SetFieldType(FieldType type)
{
    mImpl->SetFieldType(type);
}
uint64_t FieldConfig::GetU32OffsetThreshold()
{
    return mImpl->GetU32OffsetThreshold();
}
void FieldConfig::SetU32OffsetThreshold(uint64_t offsetThreshold) 
{
    mImpl->SetU32OffsetThreshold(offsetThreshold);
}
bool FieldConfig::IsMultiValue() const
{
    return mImpl->IsMultiValue();}
bool FieldConfig::IsAttributeUpdatable() const
{
    return mImpl->IsAttributeUpdatable();
}

void FieldConfig::SetUpdatableMultiValue(bool IsUpdatableMultiValue) 
{
    mImpl->SetUpdatableMultiValue(IsUpdatableMultiValue);
}
bool FieldConfig::IsVirtual() const
{
    return mImpl->IsVirtual();
}
void FieldConfig::SetVirtual(bool isVirtual)
{
    mImpl->SetVirtual(isVirtual);
}

bool FieldConfig::IsBinary() const
{
    return mImpl->IsBinary();
}
void FieldConfig::SetBinary(bool isBinary)
{
    mImpl->SetBinary(isBinary);
}

void FieldConfig::SetCompressType(const string& compressStr)
{
    mImpl->SetCompressType(compressStr);
}
bool FieldConfig::IsUniqEncode() const
{
    return mImpl->IsUniqEncode();
}

void FieldConfig::SetUniqEncode(bool isUniqEncode)
{
    mImpl->SetUniqEncode(isUniqEncode);
}
    
CompressTypeOption FieldConfig::GetCompressType() const
{
    return mImpl->GetCompressType();
}

void FieldConfig::SetDefaultValue(const string& defaultValue)
{
    mImpl->SetDefaultValue(defaultValue);
}

const string& FieldConfig::GetDefaultValue() const
{
    return mImpl->GetDefaultValue();
}

void FieldConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}
void FieldConfig::AssertEqual(const FieldConfig& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}

FieldType FieldConfig::StrToFieldType(const string& typeStr)
{
    return FieldConfigImpl::StrToFieldType(typeStr);
}
const char* FieldConfig::FieldTypeToStr(FieldType fieldType)
{
    return FieldConfigImpl::FieldTypeToStr(fieldType);
}
bool FieldConfig::IsIntegerType(FieldType fieldType)
{
    return FieldConfigImpl::IsIntegerType(fieldType);
}
void FieldConfig::Check() const
{
    mImpl->Check();
}
bool FieldConfig::SupportSort() const
{
    return mImpl->SupportSort();
}

FieldConfig* FieldConfig::Clone() const
{
    FieldConfig* cloneFieldConfig = new FieldConfig();
    cloneFieldConfig->mImpl.reset(mImpl->Clone());
    return cloneFieldConfig;
}
float FieldConfig::GetDefragSlicePercent()
{
    return mImpl->GetDefragSlicePercent();
}
void FieldConfig::SetDefragSlicePercent(uint64_t defragPercent)
{
    mImpl->SetDefragSlicePercent(defragPercent);
}

void FieldConfig::SetUserDefinedParam(const util::KeyValueMap& param)
{
    mImpl->SetUserDefinedParam(param);
}
const util::KeyValueMap& FieldConfig::GetUserDefinedParam() const
{
    return mImpl->GetUserDefinedParam();
}
int32_t FieldConfig::GetFixedMultiValueCount() const
{
    return mImpl->GetFixedMultiValueCount();
}

void FieldConfig::SetMultiValue(bool multiValue)
{
    mImpl->SetMultiValue(multiValue);
}
void FieldConfig::ClearCompressType()
{
    mImpl->ClearCompressType();
}

void FieldConfig::SetFixedMultiValueCount(int32_t fixedMultiValueCount)
{
    mImpl->SetFixedMultiValueCount(fixedMultiValueCount);
}

const FieldConfigImplPtr& FieldConfig::GetImpl()
{
    return mImpl;
}

void FieldConfig::Delete()
{
    mImpl->Delete();
}

bool FieldConfig::IsDeleted() const
{
    return mImpl->IsDeleted();
}

bool FieldConfig::IsNormal() const
{
    return mImpl->IsNormal();
}

IndexStatus FieldConfig::GetStatus() const
{
    return mImpl->GetStatus();
}

void FieldConfig::SetOwnerModifyOperationId(schema_opid_t opId)
{
    mImpl->SetOwnerModifyOperationId(opId);
}

schema_opid_t FieldConfig::GetOwnerModifyOperationId() const
{
    return mImpl->GetOwnerModifyOperationId();
}


void FieldConfig::RewriteFieldType()
{
    mImpl->RewriteFieldType();
}

IE_NAMESPACE_END(config);

