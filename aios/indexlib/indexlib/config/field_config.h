#ifndef __INDEXLIB_FIELD_CONFIG_H
#define __INDEXLIB_FIELD_CONFIG_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/key_value_map.h"
#include "indexlib/config/compress_type_option.h"
#include "autil/legacy/jsonizable.h"

DECLARE_REFERENCE_CLASS(config, FieldConfigImpl);

IE_NAMESPACE_BEGIN(config);

class FieldConfig : public autil::legacy::Jsonizable
{
public:
    friend class FieldSchema;

public:
    FieldConfig(); 
    FieldConfig(const std::string& fieldName, 
                FieldType fieldType, bool multiValue);
    FieldConfig(const std::string& fieldName,FieldType fieldType, 
                bool multiValue, bool isVirtual, bool isBinary = false);
    virtual ~FieldConfig() {}
public:
    fieldid_t GetFieldId() const;
    void SetFieldId(fieldid_t id);

    const std::string& GetFieldName() const;
    void SetFieldName(const std::string& fieldName);

    const std::string& GetAnalyzerName() const;

    void SetAnalyzerName(const std::string& analyzerName);

    FieldType GetFieldType() const;
    void SetFieldType(FieldType type);

    uint64_t GetU32OffsetThreshold(); 
    void SetU32OffsetThreshold(uint64_t offsetThreshold); 

    bool IsMultiValue() const;
    bool IsAttributeUpdatable() const;

    void SetUpdatableMultiValue(bool IsUpdatableMultiValue); 

    bool IsVirtual() const;
    void SetVirtual(bool isVirtual);

    bool IsBinary() const;
    void SetBinary(bool isBinary);

    void SetCompressType(const std::string& compressStr);

    bool IsUniqEncode() const;

    void SetUniqEncode(bool isUniqEncode);
    
    CompressTypeOption GetCompressType() const;

    void SetDefaultValue(const std::string& defaultValue);

    const std::string& GetDefaultValue() const;

    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

    virtual void AssertEqual(const FieldConfig& other) const;

    static FieldType StrToFieldType(const std::string& typeStr);
    static const char* FieldTypeToStr(FieldType fieldType);
    static bool IsIntegerType(FieldType fieldType);

    void Check() const;
    bool SupportSort() const;

    FieldConfig* Clone() const;
    float GetDefragSlicePercent();
    void SetDefragSlicePercent(uint64_t defragPercent);

    void SetUserDefinedParam(const util::KeyValueMap& param);
    const util::KeyValueMap& GetUserDefinedParam() const;
    int32_t GetFixedMultiValueCount() const;

    IndexStatus GetStatus() const;
    void Delete();
    bool IsDeleted() const;
    bool IsNormal() const;
    
    void SetOwnerModifyOperationId(schema_opid_t opId);
    schema_opid_t GetOwnerModifyOperationId() const;

    void RewriteFieldType();
public:
    //for test
    void SetMultiValue(bool multiValue);
    void ClearCompressType();

    void SetFixedMultiValueCount(int32_t fixedMultiValueCount);

    const FieldConfigImplPtr& GetImpl();

protected:
    void ResetImpl(FieldConfigImpl* impl);
    
private:
    FieldConfigImplPtr mImpl;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FieldConfig);
typedef std::vector<FieldConfigPtr> FieldConfigVector;

class FieldConfigIterator
{
public:
    FieldConfigIterator(const FieldConfigVector& indexConfigs)
        : mConfigs(indexConfigs)
    {}

    FieldConfigVector::const_iterator Begin() const
    { return mConfigs.begin(); }
    
    FieldConfigVector::const_iterator End() const
    { return mConfigs.end(); }
    
private:
    FieldConfigVector mConfigs;
};
DEFINE_SHARED_PTR(FieldConfigIterator);


IE_NAMESPACE_END(config);

#endif //__INDEXLIB_FIELD_CONFIG_H
