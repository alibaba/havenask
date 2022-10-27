#ifndef __INDEXLIB_GROUP_VINT32_ENCODER_H
#define __INDEXLIB_GROUP_VINT32_ENCODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_define.h"
#include "indexlib/common/numeric_compress/int_encoder.h"

IE_NAMESPACE_BEGIN(common);

class GroupVint32Encoder : public Int32Encoder
{
public:
    GroupVint32Encoder();
    ~GroupVint32Encoder();

    static const uint32_t ENCODER_BUFFER_SIZE = MAX_RECORD_SIZE * sizeof(uint32_t) * 5 / 4;
    static const uint16_t SRC_LEN_FLAG = 0x4000;

public:
    uint32_t Encode(common::ByteSliceWriter& sliceWriter,
                    const uint32_t* src, uint32_t srcLen) const override;
    
    uint32_t Encode(uint8_t *dest, const uint32_t* src, uint32_t srcLen) const override;
    
    uint32_t Decode(uint32_t* dest, uint32_t destLen,
                    common::ByteSliceReader& sliceReader) const override;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(GroupVint32Encoder);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_GROUP_VINT32_ENCODER_H
