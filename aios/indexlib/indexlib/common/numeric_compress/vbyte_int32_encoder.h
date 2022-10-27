#ifndef __INDEXLIB_VBYTE_INT32_ENCODER_H
#define __INDEXLIB_VBYTE_INT32_ENCODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/numeric_compress/int_encoder.h"

IE_NAMESPACE_BEGIN(common);

class VbyteInt32Encoder : public Int32Encoder
{
public:
    VbyteInt32Encoder();
    ~VbyteInt32Encoder();
public:
    uint32_t Encode(common::ByteSliceWriter& sliceWriter,
                    const uint32_t* src, uint32_t srcLen) const override;
    
    uint32_t Encode(uint8_t *dest, const uint32_t* src, uint32_t srcLen) const override;
    
    uint32_t Decode(uint32_t* dest, uint32_t destLen,
                    common::ByteSliceReader& sliceReader) const override;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VbyteInt32Encoder);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_VBYTE_INT32_ENCODER_H
