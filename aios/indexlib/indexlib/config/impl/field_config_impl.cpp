#include <autil/legacy/any.h>
#include <autil/legacy/jsonizable.h>
#include <autil/legacy/json.h>
#include "indexlib/config/impl/field_config_impl.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/schema_configurator.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

IE_NAMESPACE_USE(util);

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);

IE_LOG_SETUP(config, FieldConfigImpl);

FieldConfigImpl::FieldConfigImpl()
    : mFieldId(INVALID_FIELDID)
    , mFieldType(ft_unknown)
    , mU32OffsetThreshold(FIELD_U32OFFSET_THRESHOLD_MAX)
    , mMultiValue(false)
    , mUpdatableMultiValue(false)
    , mVirtual(false)
    , mAttrUpdatable(false)
    , mBinaryField(false)
    , mDefaultStrValue(FIELD_DEFAULT_STR_VALUE)
    , mDefragSlicePercent(FIELD_DEFAULT_DEFRAG_SLICE_PERCENT)
    , mFixedMultiValueCount(-1)
    , mStatus(is_normal)
    , mOwnerOpId(INVALID_SCHEMA_OP_ID)      
{
    mAttrUpdatable = IsFieldUpdatable(
            mFieldType, mMultiValue, mUpdatableMultiValue);
}
    
FieldConfigImpl::FieldConfigImpl(const std::string& fieldName, 
                                 FieldType fieldType, bool multiValue)
    : mFieldId(INVALID_FIELDID)
    , mFieldName(fieldName)
    , mFieldType(fieldType)
    , mU32OffsetThreshold(FIELD_U32OFFSET_THRESHOLD_MAX)
    , mMultiValue(multiValue)
    , mUpdatableMultiValue(false)
    , mVirtual(false)
    , mAttrUpdatable(false)
    , mBinaryField(false)
    , mDefaultStrValue(FIELD_DEFAULT_STR_VALUE)
    , mDefragSlicePercent(FIELD_DEFAULT_DEFRAG_SLICE_PERCENT)
    , mFixedMultiValueCount(-1)
    , mStatus(is_normal)
    , mOwnerOpId(INVALID_SCHEMA_OP_ID)      
{
    if (fieldType == ft_location || fieldType == ft_line ||
        fieldType == ft_polygon)
    {
        mMultiValue = true;
    }
    mAttrUpdatable = IsFieldUpdatable(
            mFieldType, mMultiValue, mUpdatableMultiValue);
}

FieldConfigImpl::FieldConfigImpl(const std::string& fieldName,FieldType fieldType, 
                                 bool multiValue, bool isVirtual, bool isBinary)
    : mFieldId(INVALID_FIELDID)
    , mFieldName(fieldName)
    , mFieldType(fieldType)
    , mU32OffsetThreshold(FIELD_U32OFFSET_THRESHOLD_MAX)
    , mMultiValue(multiValue)
    , mUpdatableMultiValue(false)
    , mVirtual(isVirtual)
    , mAttrUpdatable(false)
    , mBinaryField(isBinary)
    , mDefaultStrValue(FIELD_DEFAULT_STR_VALUE)
    , mDefragSlicePercent(FIELD_DEFAULT_DEFRAG_SLICE_PERCENT)
    , mFixedMultiValueCount(-1)
    , mStatus(is_normal)
    , mOwnerOpId(INVALID_SCHEMA_OP_ID)      
{
    if (fieldType == ft_location || fieldType == ft_line ||
        fieldType == ft_polygon)
    {
        mMultiValue = true;
    }

    mAttrUpdatable = IsFieldUpdatable(
            mFieldType, mMultiValue, mUpdatableMultiValue);
}

void FieldConfigImpl::Check() const
{
    CheckUniqEncode();
    CheckEquivalentCompress();
    CheckBlockFpEncode();
    CheckFp16Encode();
    CheckFloatInt8Encode();
    CheckDefaultAttributeValue();
    CheckFixedLengthMultiValue();
}

bool FieldConfigImpl::IsIntegerType(FieldType fieldType)
{
    switch(fieldType)
    {
    case ft_int32:
    case ft_int64:
    case ft_uint32:
    case ft_uint64:
    case ft_int8:
    case ft_uint8:
    case ft_int16:
    case ft_uint16:
        return true;
    default:
        return false;
    }
    return false;
}

void FieldConfigImpl::CheckUniqEncode() const
{
    if (mCompressType.HasUniqEncodeCompress())
    {
        if (mMultiValue || mFieldType == ft_string)
        {
            //do nothing
        }
        else
        {
            stringstream ss;
            ss << "uniqEncode invalid for field " << mFieldName;
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }
    }
}

void FieldConfigImpl::CheckBlockFpEncode() const
{
    
    if (!mCompressType.HasBlockFpEncodeCompress())
    {
        return;
    }
    if (!mMultiValue || mFieldType != ft_float || GetFixedMultiValueCount() == -1)
    {
        stringstream ss;
        ss << "BlockFpEncode invalid for field " << mFieldName;
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }
}

void FieldConfigImpl::CheckFp16Encode() const
{
    if (mFieldType == ft_fp16)
    {
        return;
    }
    if (!mCompressType.HasFp16EncodeCompress())
    {
        return;
    }
    if (mFieldType != ft_float)
    {
        stringstream ss;
        ss << "Fp16Encode invalid for field " << mFieldName;
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }
    if (!mMultiValue && mCompressType.HasEquivalentCompress())
    {
        INDEXLIB_FATAL_ERROR(Schema, "can not use fp16 & equalCompress for field[%s] at the same time",
                             mFieldName.c_str());
    }
}


void FieldConfigImpl::CheckFloatInt8Encode() const
{
    if (mFieldType == ft_fp8)
    {
        return;
    }

    if (!mCompressType.HasInt8EncodeCompress())
    {
        return;
    }
    if (mFieldType != ft_float)
    {
        stringstream ss;
        ss << "Int8FloatEncode invalid for field " << mFieldName;
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }

    if (!mMultiValue && mCompressType.HasEquivalentCompress())
    {
        INDEXLIB_FATAL_ERROR(Schema,
                             "can not use int8Compress & equalCompress for field[%s] at the same time",
                             mFieldName.c_str());
    }
}

void FieldConfigImpl::CheckEquivalentCompress() const
{
    //only not equal type allows to use compress
    if (mCompressType.HasEquivalentCompress())
    { 
        if(mMultiValue 
           || mFieldType == ft_integer 
           || mFieldType == ft_uint32
           || mFieldType == ft_long
           || mFieldType == ft_uint64
           || mFieldType == ft_int8
           || mFieldType == ft_uint8
           || mFieldType == ft_int16
           || mFieldType == ft_uint16
           || mFieldType == ft_float
           || mFieldType == ft_fp8
           || mFieldType == ft_fp16
           || mFieldType == ft_double
           || mFieldType == ft_string)
        {
            //do nothing
        }
        else
        {
            INDEXLIB_FATAL_ERROR(Schema,
                    "equivalent compress invalid for field [%s]",
                    mFieldName.c_str());
        }
    }
}

void FieldConfigImpl::CheckDefaultAttributeValue() const
{
    if (!mMultiValue && (mFieldType == ft_integer 
                         || mFieldType == ft_uint32
                         || mFieldType == ft_long
                         || mFieldType == ft_uint64
                         || mFieldType == ft_int8
                         || mFieldType == ft_uint8
                         || mFieldType == ft_double
                         || mFieldType == ft_float
                         || mFieldType == ft_fp8
                         || mFieldType == ft_fp16
                         || mFieldType == ft_int16
                         || mFieldType == ft_uint16
                         || mFieldType == ft_string))
    {
        return;
    }

    if (mDefaultStrValue != FIELD_DEFAULT_STR_VALUE)
    {
        INDEXLIB_FATAL_ERROR(Schema, 
                             "Field [%s] not support set defaultStrValue",
                             mFieldName.c_str());
    }
}

void FieldConfigImpl::CheckFixedLengthMultiValue() const
{
    if (mFixedMultiValueCount == -1)
    {
        return;
    }

    if (mMultiValue)
    {
        if(mFieldType == ft_integer 
           || mFieldType == ft_uint32
           || mFieldType == ft_long
           || mFieldType == ft_uint64
           || mFieldType == ft_int8
           || mFieldType == ft_uint8
           || mFieldType == ft_double
           || mFieldType == ft_float
           || mFieldType == ft_int16
           || mFieldType == ft_uint16)
        {
            if (mFixedMultiValueCount == 0)
            {
                INDEXLIB_FATAL_ERROR(Schema,
                        "Field [%s] does not support "
                        "set fixed_multi_value_count = 0",
                        mFieldName.c_str()); 
            }
            return;
        }
        INDEXLIB_FATAL_ERROR(Schema,
                             "Field [%s] , FieldType[%d] does not support "
                             "set fixed_multi_value_count",
                             mFieldName.c_str(), int(mFieldType));        
    }
    // single value
    if (mFieldType == ft_string)
    {
        if (mFixedMultiValueCount == 0)
        {
            INDEXLIB_FATAL_ERROR(Schema,
                    "Field [%s] does not support "
                    "set fixed_multi_value_count = 0",
                    mFieldName.c_str());             
        }
        return;
    }
    INDEXLIB_FATAL_ERROR(Schema,
                         "Field [%s] , FieldType[%d] does not support "
                         "set fixed_multi_value_count",
                         mFieldName.c_str(), int(mFieldType)); 
}

void FieldConfigImpl::AssertEqual(const FieldConfigImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mFieldId, other.mFieldId, "FieldId not equal");
    IE_CONFIG_ASSERT_EQUAL(mMultiValue, other.mMultiValue, "MultiValue not equal");
    IE_CONFIG_ASSERT_EQUAL(mUpdatableMultiValue, other.mUpdatableMultiValue, "UpdatableMultiValue not equal");
    IE_CONFIG_ASSERT_EQUAL(mBinaryField, other.mBinaryField, "BinaryField not equal");
    IE_CONFIG_ASSERT_EQUAL(mFieldName, other.mFieldName, "FieldName not equal");
    IE_CONFIG_ASSERT_EQUAL(mAnalyzerName, other.mAnalyzerName, "AnalyzerName not equal"); 
    IE_CONFIG_ASSERT_EQUAL(mDefragSlicePercent, other.mDefragSlicePercent, "defrag slice percent not equal");
    IE_CONFIG_ASSERT_EQUAL(mFixedMultiValueCount, other.mFixedMultiValueCount, "fixed_multi_value_count not equal"); 
    IE_CONFIG_ASSERT_EQUAL(mUserDefinedParam, other.mUserDefinedParam, "user defined param not equal"); 
    mCompressType.AssertEqual(other.mCompressType);
    CheckFieldTypeEqual(other);
}

void FieldConfigImpl::CheckFieldTypeEqual(const FieldConfigImpl& other) const
{
    if (mMultiValue)
    {
        IE_CONFIG_ASSERT_EQUAL(mFieldType, other.mFieldType, "FieldType not equal");
        return;
    }
    // only single float will be rewrited
    if (mCompressType.HasFp16EncodeCompress())
    {
        if ((mFieldType == ft_fp16 || mFieldType == ft_float)
            && (other.mFieldType == ft_fp16 || other.mFieldType == ft_float))
        {
            return;
        }
    }

    if (mCompressType.HasInt8EncodeCompress())
    {
        if ((mFieldType == ft_fp8 || mFieldType == ft_float)
            && (other.mFieldType == ft_fp8 || other.mFieldType == ft_float))
        {
            return;
        }
    }
    
    IE_CONFIG_ASSERT_EQUAL(mFieldType, other.mFieldType, "FieldType not equal");
}

void FieldConfigImpl::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) 
{
    json.Jsonize(FIELD_NAME, mFieldName);
    json.Jsonize(FIELD_BINARY, mBinaryField, mBinaryField);
    if (json.GetMode() == TO_JSON) 
    {
        if (mMultiValue)
        {
            json.Jsonize(FIELD_MULTI_VALUE, mMultiValue);
        }

        if (mFixedMultiValueCount != -1)
        {
            json.Jsonize(FIELD_FIXED_MULTI_VALUE_COUNT, mFixedMultiValueCount); 
        }

        if (mUpdatableMultiValue)
        {
            json.Jsonize(FIELD_UPDATABLE_MULTI_VALUE, mUpdatableMultiValue);
        }
   
        if (mU32OffsetThreshold != FIELD_U32OFFSET_THRESHOLD_MAX)
        {
            json.Jsonize(FIELD_U32OFFSET_THRESHOLD, mU32OffsetThreshold);
        }
       
        if (mCompressType.HasCompressOption())
        {
            string compressStr = mCompressType.GetCompressStr();
            assert(!compressStr.empty());
            json.Jsonize(COMPRESS_TYPE, compressStr);
        }

        string typeStr = FieldTypeToStr(mFieldType);
        json.Jsonize(FIELD_TYPE, typeStr);
        if (mFieldType == ft_text)
        {
            json.Jsonize(FIELD_ANALYZER, mAnalyzerName);
        }

        if (mVirtual)
        {
            json.Jsonize(FIELD_VIRTUAL, mVirtual);
        }

        if (mDefaultStrValue != FIELD_DEFAULT_STR_VALUE)
        {
            json.Jsonize(FIELD_DEFAULT_VALUE, mDefaultStrValue);
        }

        if (mDefragSlicePercent != FIELD_DEFAULT_DEFRAG_SLICE_PERCENT)
        {
            json.Jsonize(FIELD_DEFRAG_SLICE_PERCENT, mDefragSlicePercent);
        }

        if (!mUserDefinedParam.empty())
        {
            json.Jsonize(FIELD_USER_DEFINED_PARAM, mUserDefinedParam);
        }
	  
    }
    else
    {
        string typeStr;
        json.Jsonize(FIELD_TYPE, typeStr);
        mFieldType = StrToFieldType(typeStr);

        bool uniqEncode = false;
        json.Jsonize(FIELD_UNIQ_ENCODE, uniqEncode, uniqEncode);

        string compressType;
        json.Jsonize(COMPRESS_TYPE, compressType, compressType);
        mCompressType.Init(compressType);
        if (uniqEncode)
        {
            mCompressType.SetUniqEncode();
        }

        json.Jsonize(FIELD_MULTI_VALUE, mMultiValue, mMultiValue);
        json.Jsonize(FIELD_UPDATABLE_MULTI_VALUE, mUpdatableMultiValue, mUpdatableMultiValue);
        json.Jsonize(FIELD_U32OFFSET_THRESHOLD, mU32OffsetThreshold, mU32OffsetThreshold);
        json.Jsonize(FIELD_DEFRAG_SLICE_PERCENT, mDefragSlicePercent, mDefragSlicePercent);
        if (mU32OffsetThreshold > FIELD_U32OFFSET_THRESHOLD_MAX)
        {
            mU32OffsetThreshold = FIELD_U32OFFSET_THRESHOLD_MAX;
        }
        json.Jsonize(FIELD_FIXED_MULTI_VALUE_COUNT, mFixedMultiValueCount, mFixedMultiValueCount); 
        json.Jsonize(FIELD_VIRTUAL, mVirtual, mVirtual);
        json.Jsonize(FIELD_DEFAULT_VALUE, mDefaultStrValue, mDefaultStrValue);
        mAttrUpdatable = IsFieldUpdatable(
                mFieldType, mMultiValue, mUpdatableMultiValue);
        json.Jsonize(FIELD_USER_DEFINED_PARAM, mUserDefinedParam, mUserDefinedParam);
    }
}

FieldType FieldConfigImpl::StrToFieldType(const string& typeStr)
{
    if (!strcasecmp(typeStr.c_str(), "text"))
    {
        return ft_text;
    } 
    else if (!strcasecmp(typeStr.c_str(), "string"))
    {
        return ft_string;
    } 
    else if (!strcasecmp(typeStr.c_str(), "integer"))
    {
        return ft_integer;
    }
    else if (!strcasecmp(typeStr.c_str(), "enum"))
    {
        return ft_enum;
    }
    else if (!strcasecmp(typeStr.c_str(), "float"))
    {
        return ft_float;
    }
    else if (!strcasecmp(typeStr.c_str(), "fp8"))
    {
        return ft_fp8;
    }
    else if (!strcasecmp(typeStr.c_str(), "fp16"))
    {
        return ft_fp16;
    }
    else if (!strcasecmp(typeStr.c_str(), "long"))
    {
        return ft_long;
    }
    else if (!strcasecmp(typeStr.c_str(), "time"))
    {
        return ft_time;
    }
    else if (!strcasecmp(typeStr.c_str(), "location"))
    {
        return ft_location;
    }
    else if (!strcasecmp(typeStr.c_str(), "polygon"))
    {
        return ft_polygon;
    }
    else if (!strcasecmp(typeStr.c_str(), "line"))
    {
        return ft_line;
    }
    else if (!strcasecmp(typeStr.c_str(), "online"))
    {
        return ft_online;
    }
    else if (!strcasecmp(typeStr.c_str(), "property"))
    {
        return ft_property;
    } 
    else if (!strcasecmp(typeStr.c_str(), "uint8"))
    {
        return ft_uint8;
    }    
    else if (!strcasecmp(typeStr.c_str(), "int8"))
    {
        return ft_int8;
    }
    else if (!strcasecmp(typeStr.c_str(), "uint16"))
    {
        return ft_uint16;
    }    
    else if (!strcasecmp(typeStr.c_str(), "int16"))
    {
        return ft_int16;
    }
    else if (!strcasecmp(typeStr.c_str(), "uint32"))
    {
        return ft_uint32;
    }    
    else if (!strcasecmp(typeStr.c_str(), "int32"))
    {
        return ft_int32;
    }    
    else if (!strcasecmp(typeStr.c_str(), "uint64"))
    {
        return ft_uint64;
    }    
    else if (!strcasecmp(typeStr.c_str(), "int64"))
    {
        return ft_int64;
    }    
    else if (!strcasecmp(typeStr.c_str(), "hash_64"))
    {
        return ft_hash_64;
    }
    else if (!strcasecmp(typeStr.c_str(), "hash_128"))
    {
        return ft_hash_128;
    }
    else if (!strcasecmp(typeStr.c_str(), "double"))
    {
        return ft_double;
    }
    else if (!strcasecmp(typeStr.c_str(), "raw"))
    {
        return ft_raw;
    }    

    stringstream ss;
    ss << "Unknown field_type: " << typeStr << ", support field_type are: ";
    for (int ft = 0; ft < (int)ft_unknown; ++ft)
    {
        ss << FieldTypeToStr((FieldType)ft) << ",";
    }
    
    INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    return ft_unknown;
}

const char* FieldConfigImpl::FieldTypeToStr(FieldType fieldType) 
{
    switch (fieldType)
    {
    case ft_text: 
        return "TEXT";
    case ft_string:
        return "STRING";
    case ft_enum: 
        return "ENUM";
    case ft_integer: 
        return "INTEGER";
    case ft_fp8:
    case ft_fp16: 
    case ft_float: 
        return "FLOAT";
    case ft_long: 
        return "LONG";
    case ft_time: 
        return "TIME";
    case ft_location: 
        return "LOCATION";
    case ft_line: 
        return "LINE";
    case ft_polygon: 
        return "POLYGON";
    case ft_online: 
        return "ONLINE";
    case ft_property: 
        return "PROPERTY";
    case ft_uint32:
        return "UINT32";
    case ft_uint64:
        return "UINT64";
    case ft_hash_64:
        return "HASH_64";
    case ft_hash_128:
        return "HASH_128";
    case ft_int8:
        return "INT8";
    case ft_uint8:
        return "UINT8";
    case ft_int16:
        return "INT16";
    case ft_uint16:
        return "UINT16";
    case ft_double:
        return "DOUBLE";
    case ft_raw:
        return "RAW";
    default:
        return "UNKNOWN";
    }
    return "UNKNOWN";
}

bool FieldConfigImpl::SupportSort() const 
{
    if (mMultiValue)
    {
        return false;
    }
    return mFieldType == ft_integer || mFieldType == ft_uint32
        || mFieldType == ft_long    || mFieldType == ft_uint64
        || mFieldType == ft_int8    || mFieldType == ft_uint8
        || mFieldType == ft_int16   || mFieldType == ft_uint16
        || mFieldType == ft_double  || mFieldType == ft_float
        || mFieldType == ft_fp8     || mFieldType == ft_fp16;
}

FieldConfigImpl* FieldConfigImpl::Clone() const
{
    FieldConfigImpl* fieldConf = new FieldConfigImpl();
    autil::legacy::Any any = autil::legacy::ToJson(*this);
    FromJson(*fieldConf, any);
    return fieldConf;    
}

void FieldConfigImpl::SetUserDefinedParam(const KeyValueMap& param)
{
    mUserDefinedParam = param;
}

const KeyValueMap& FieldConfigImpl::GetUserDefinedParam() const
{
    return mUserDefinedParam;
}

void FieldConfigImpl::Delete()
{
    mStatus = is_deleted;
}

void FieldConfigImpl::RewriteFieldType()
{
    if (mFieldType != ft_float || mMultiValue || mFixedMultiValueCount != -1) {
        return;
    }
  
    if (mCompressType.HasInt8EncodeCompress())
    {
        mFieldType = ft_fp8;
    }
    else if (mCompressType.HasFp16EncodeCompress())
    {
        mFieldType = ft_fp16;
    }
}

IE_NAMESPACE_END(config);

