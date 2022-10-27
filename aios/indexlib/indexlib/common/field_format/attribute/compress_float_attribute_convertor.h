#ifndef __INDEXLIB_COMPRESS_FLOAT_ATTRIBUTE_CONVERTOR_H
#define __INDEXLIB_COMPRESS_FLOAT_ATTRIBUTE_CONVERTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_convertor.h"
#include "indexlib/config/compress_type_option.h"

IE_NAMESPACE_BEGIN(common);

class CompressFloatAttributeConvertor : public VarNumAttributeConvertor<float>
{
public:
    CompressFloatAttributeConvertor(
        config::CompressTypeOption compressType,
        bool needHash, const std::string& fieldName, int32_t fixedValueCount);
    
    ~CompressFloatAttributeConvertor();

protected:
    autil::ConstString InnerEncode(
        const autil::ConstString& originalData,
        const std::vector<autil::ConstString> &attrDatas,
        autil::mem_pool::Pool *memPool,
        std::string &resultStr, char *outBuffer, EncodeStatus &status) override;

private:
    config::CompressTypeOption mCompressType;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CompressFloatAttributeConvertor);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_COMPRESS_FLOAT_ATTRIBUTE_CONVERTOR_H
