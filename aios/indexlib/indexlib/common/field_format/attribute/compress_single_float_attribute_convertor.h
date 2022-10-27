#ifndef __INDEXLIB_COMPRESS_SINGLE_FLOAT_ATTRIBUTE_CONVERTOR_H
#define __INDEXLIB_COMPRESS_SINGLE_FLOAT_ATTRIBUTE_CONVERTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/fp16_encoder.h"
#include "indexlib/util/float_int8_encoder.h"
#include "indexlib/config/compress_type_option.h"
#include "indexlib/common/field_format/attribute/single_value_attribute_convertor.h"

IE_NAMESPACE_BEGIN(common);

template <typename T>
class CompressSingleFloatAttributeConvertor : public SingleValueAttributeConvertor<T>
{
public:
    CompressSingleFloatAttributeConvertor(
            config::CompressTypeOption compressType,
            const std::string& fieldName)
        : SingleValueAttributeConvertor<T>(false, fieldName)
        , mCompressType(compressType)
    {
    }

    virtual ~CompressSingleFloatAttributeConvertor() {}
private:
    autil::ConstString InnerEncode(
            const autil::ConstString &attrData,
            autil::mem_pool::Pool *memPool,
            std::string &resultStr, char *outBuffer, 
            AttributeConvertor::EncodeStatus &status) override;
private:
    config::CompressTypeOption mCompressType;

private:
    IE_LOG_DECLARE();
};

//////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE(common, CompressSingleFloatAttributeConvertor);
template<typename T>
inline autil::ConstString CompressSingleFloatAttributeConvertor<T>::InnerEncode(
        const autil::ConstString &attrData, autil::mem_pool::Pool *memPool,
        std::string &resultStr, char *outBuffer, AttributeConvertor::EncodeStatus &status)
{
    float value;
    if(!StrToT(attrData, value))
    {
        status = AttributeConvertor::EncodeStatus::ES_TYPE_ERROR;
        value = (float)0.0;
        IE_LOG(DEBUG, "convert attribute[%s] error value:[%s]",
               this->mFieldName.c_str(), attrData.c_str());
        ERROR_COLLECTOR_LOG(ERROR, "convert attribute[%s] error value:[%s]", 
                            this->mFieldName.c_str(), attrData.c_str());
    }

    T *output = (T*)(this->allocate(memPool, resultStr, outBuffer, sizeof(T)));
    if (mCompressType.HasFp16EncodeCompress()) {
        util::Fp16Encoder::Encode(value, (char*)output);
    } else if (mCompressType.HasInt8EncodeCompress()) {
        util::FloatInt8Encoder::Encode(mCompressType.GetInt8AbsMax(), value, (char*)output);
    } else {
        IE_LOG(ERROR, "invalid compressType[%s] for field[%s]", 
               this->mCompressType.GetCompressStr().c_str(), this->mFieldName.c_str());
        ERROR_COLLECTOR_LOG(ERROR, "invalid compressType[%s] for field[%s]", 
                            this->mCompressType.GetCompressStr().c_str(), this->mFieldName.c_str());
        status = AttributeConvertor::EncodeStatus::ES_TYPE_ERROR;
    }
    return autil::ConstString((char*)output, sizeof(T));
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_COMPRESS_SINGLE_FLOAT_ATTRIBUTE_CONVERTOR_H
