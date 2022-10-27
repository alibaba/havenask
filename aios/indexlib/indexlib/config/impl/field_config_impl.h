#ifndef __INDEXLIB_FIELD_CONFIG_IMPL_H
#define __INDEXLIB_FIELD_CONFIG_IMPL_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/misc/exception.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/config_define.h"
#include "indexlib/config/compress_type_option.h"
#include "indexlib/util/key_value_map.h"

IE_NAMESPACE_BEGIN(config);

class FieldConfigImpl : public autil::legacy::Jsonizable
{
public:
    friend class FieldSchema;

public:
    FieldConfigImpl();
    FieldConfigImpl(const std::string& fieldName, 
                FieldType fieldType, bool multiValue);
    FieldConfigImpl(const std::string& fieldName,FieldType fieldType, 
                bool multiValue, bool isVirtual, bool isBinary = false);
    virtual ~FieldConfigImpl() {}
public:
    fieldid_t GetFieldId() const {return mFieldId;}
    void SetFieldId(fieldid_t id) { mFieldId = id; }

    const std::string& GetFieldName() const {return mFieldName;}
    void SetFieldName(const std::string& fieldName) { mFieldName = fieldName; }

    const std::string& GetAnalyzerName() const {return mAnalyzerName;}

    void SetAnalyzerName(const std::string& analyzerName) { mAnalyzerName = analyzerName; } 

    FieldType GetFieldType() const {return mFieldType;}
    void SetFieldType(FieldType type)
    { 
        mFieldType = type;
        mAttrUpdatable = IsFieldUpdatable(
                mFieldType, mMultiValue, mUpdatableMultiValue);
    }

    uint64_t GetU32OffsetThreshold() {return mU32OffsetThreshold;}
    void SetU32OffsetThreshold(uint64_t offsetThreshold) 
    { 
        mU32OffsetThreshold = offsetThreshold;
    }
    bool IsMultiValue() const {return mMultiValue;}
    bool IsAttributeUpdatable() const { return mAttrUpdatable; }

    void SetUpdatableMultiValue(bool IsUpdatableMultiValue) 
    { 
        mUpdatableMultiValue = IsUpdatableMultiValue; 
        mAttrUpdatable = IsFieldUpdatable(
                mFieldType, mMultiValue, mUpdatableMultiValue);
    }

    bool IsVirtual() const {return mVirtual;}
    void SetVirtual(bool isVirtual) { mVirtual = isVirtual;}

    bool IsBinary() const {return mBinaryField;}
    void SetBinary(bool isBinary) { mBinaryField = isBinary;}

    void SetCompressType(const std::string& compressStr)
    { mCompressType.Init(compressStr); }
    bool IsUniqEncode() const
    {
        return mCompressType.HasUniqEncodeCompress();
    }

    void SetUniqEncode(bool isUniqEncode)
    {
        if (isUniqEncode)
        {
            mCompressType.SetUniqEncode();
        }
    }

    CompressTypeOption GetCompressType() const
    { return mCompressType; }

    void SetDefaultValue(const std::string& defaultValue)
    { mDefaultStrValue = defaultValue; }

    const std::string& GetDefaultValue() const
    { return mDefaultStrValue; }

    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    virtual void AssertEqual(const FieldConfigImpl& other) const;

    static FieldType StrToFieldType(const std::string& typeStr);
    static const char* FieldTypeToStr(FieldType fieldType);
    static bool IsIntegerType(FieldType fieldType);
    void Check() const;
    bool SupportSort() const;

    FieldConfigImpl* Clone() const;
    float GetDefragSlicePercent() { return (float)mDefragSlicePercent / 100; }
    void SetDefragSlicePercent(uint64_t defragPercent)
    { mDefragSlicePercent = defragPercent; }

    void SetUserDefinedParam(const util::KeyValueMap& param);
    const util::KeyValueMap& GetUserDefinedParam() const;
    int32_t GetFixedMultiValueCount() const
    {
        return mFixedMultiValueCount;
    }

    IndexStatus GetStatus() const { return mStatus; }
    void Delete();
    bool IsDeleted() const { return mStatus == is_deleted; }
    bool IsNormal() const { return mStatus == is_normal; }

    void SetOwnerModifyOperationId(schema_opid_t opId) { mOwnerOpId = opId; }
    schema_opid_t GetOwnerModifyOperationId() const { return mOwnerOpId; }

    void RewriteFieldType();
    
public:
    //for test
    void SetMultiValue(bool multiValue)
    { 
        mMultiValue = multiValue;
        mAttrUpdatable = IsFieldUpdatable(
                mFieldType, mMultiValue, mUpdatableMultiValue);
    }
    void ClearCompressType()
    {
        mCompressType.ClearCompressType();
    }

    void SetFixedMultiValueCount(int32_t fixedMultiValueCount)
    {
        mFixedMultiValueCount = fixedMultiValueCount;
    }

private:
    void CheckUniqEncode() const;
    void CheckEquivalentCompress() const;
    void CheckBlockFpEncode() const;
    void CheckFp16Encode() const;
    void CheckFloatInt8Encode() const;
    void CheckDefaultAttributeValue() const;
    void CheckFixedLengthMultiValue() const;
    void CheckFieldTypeEqual(const FieldConfigImpl& other) const;

    bool IsFieldUpdatable(FieldType fieldType, 
                          bool multiValue, bool updatableMultiValue) const
    {
        if (multiValue && !updatableMultiValue)
        {
            return false;
        }

        return fieldType == ft_integer || fieldType == ft_uint32
            || fieldType == ft_long    || fieldType == ft_uint64
            || fieldType == ft_int8    || fieldType == ft_uint8
            || fieldType == ft_int16   || fieldType == ft_uint16
            || fieldType == ft_double  || fieldType == ft_float
            || fieldType == ft_fp8     || fieldType == ft_fp16
            || (fieldType == ft_string && updatableMultiValue);
    }

protected:
    fieldid_t mFieldId;
    std::string mFieldName;
    FieldType mFieldType;
    uint64_t mU32OffsetThreshold;
    bool mMultiValue;
    bool mUpdatableMultiValue;
    bool mVirtual;
    bool mAttrUpdatable;
    bool mBinaryField;
    std::string mAnalyzerName;
    CompressTypeOption mCompressType;
    std::string mDefaultStrValue;
    uint64_t mDefragSlicePercent;
    int32_t mFixedMultiValueCount;
    util::KeyValueMap mUserDefinedParam;
    IndexStatus mStatus;
    schema_opid_t mOwnerOpId;
    
private:
    friend class FieldConfigTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FieldConfigImpl);
IE_NAMESPACE_END(config);

#endif //__INDEXLIB_FIELD_CONFIG_IMPL_H
