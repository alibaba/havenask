#include "indexlib/common/field_format/attribute/compress_float_attribute_convertor.h"
#include "indexlib/util/block_fp_encoder.h"
#include "indexlib/util/fp16_encoder.h"
#include "indexlib/util/float_int8_encoder.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, CompressFloatAttributeConvertor);

CompressFloatAttributeConvertor::CompressFloatAttributeConvertor(
    CompressTypeOption compressType,
    bool needHash,  const string& fieldName, int32_t fixedValueCount)
    : VarNumAttributeConvertor(needHash, fieldName, fixedValueCount)
    , mCompressType(compressType)
{
    assert(fixedValueCount != -1);
    assert(mCompressType.HasFp16EncodeCompress() ||
           mCompressType.HasBlockFpEncodeCompress() ||
           mCompressType.HasInt8EncodeCompress());
}

CompressFloatAttributeConvertor::~CompressFloatAttributeConvertor() 
{
}

ConstString CompressFloatAttributeConvertor::InnerEncode(
    const ConstString& originalValue, const vector<ConstString> &vec,
    Pool *memPool, string &resultStr, char *outBuffer, EncodeStatus &status)
{
    size_t tokenNum = vec.size();
    tokenNum = AdjustCount(tokenNum);
    
    size_t bufSize = sizeof(uint64_t);// hash value
    if (mCompressType.HasBlockFpEncodeCompress()) {
        bufSize += BlockFpEncoder::GetEncodeBytesLen(tokenNum);
    } else if (mCompressType.HasFp16EncodeCompress()) {
        bufSize += Fp16Encoder::GetEncodeBytesLen(tokenNum);
    } else if (mCompressType.HasInt8EncodeCompress()) {
        bufSize += FloatInt8Encoder::GetEncodeBytesLen(tokenNum);
    }

    char *begin = (char*)allocate(memPool, resultStr, outBuffer, bufSize);
    char *buffer = begin;
    AppendHashAndCount(originalValue, (uint32_t)tokenNum, buffer);

    vector<float> floatVec;
    floatVec.resize(tokenNum, float());
    for (size_t i = 0; i < tokenNum; ++i)
    {
        float& value = floatVec[i];
        if(!StrToT(vec[i], value))
        {
            status = ES_TYPE_ERROR;
            IE_LOG(DEBUG, "convert attribute[%s] error multi_value:[%s]",
                   mFieldName.c_str(), originalValue.c_str());
            ERROR_COLLECTOR_LOG(DEBUG, "convert attribute[%s] error multi_value:[%s]",
                                mFieldName.c_str(), originalValue.c_str());
        }
    }

    size_t encodeLen = bufSize - (buffer - begin);
    int32_t compLen = -1;
    if (mCompressType.HasBlockFpEncodeCompress()) {
        compLen = BlockFpEncoder::Encode(
            (const float*)floatVec.data(), floatVec.size(), buffer, encodeLen);
    } else if (mCompressType.HasFp16EncodeCompress()) {
        compLen = Fp16Encoder::Encode(
            (const float*)floatVec.data(), floatVec.size(), buffer, encodeLen);
    } else if (mCompressType.HasInt8EncodeCompress()) {
        compLen = FloatInt8Encoder::Encode(mCompressType.GetInt8AbsMax(),
            (const float*)floatVec.data(), floatVec.size(), buffer, encodeLen);
    }


    if (compLen == -1)
    {
        ERROR_COLLECTOR_LOG(ERROR, "convert attribute [%s] error, multi_value:[%s], "
                            "compress type[%s]",
                            mFieldName.c_str(), originalValue.c_str(),
                            mCompressType.GetCompressStr().c_str());
        return ConstString::EMPTY_STRING;
    }
    assert((size_t)(compLen + (buffer - begin)) == bufSize);
    return ConstString(begin, bufSize);
}

IE_NAMESPACE_END(common);

