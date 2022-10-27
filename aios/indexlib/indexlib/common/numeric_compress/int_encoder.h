#ifndef __INDEXLIB_INT_ENCODER_H
#define __INDEXLIB_INT_ENCODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/byte_slice_writer.h"
#include "indexlib/common/byte_slice_reader.h"

IE_NAMESPACE_BEGIN(common);

template<typename T>
class IntEncoder
{
public:
    IntEncoder() {}
    virtual ~IntEncoder() {}
public:
    /**
     * Encode T array to byte slice list
     * @param sliceWriter where encoded bytes stored
     * @param src source T array
     * @param srcLen length of /src/
     * @return length of encoded byte array
     */
    virtual uint32_t Encode(common::ByteSliceWriter& sliceWriter,
                            const T* src, uint32_t srcLen) const = 0;
    virtual uint32_t Encode(uint8_t *dest, const T* src, 
                            uint32_t srcLen) const { assert(false); return 0; }

    /**
     * Decode encoded bytes to T array
     * @param dest where decoded T values stored
     * @param destLen length of /dest/
     * @param sliceReader where encoded bytes read from
     * @return count of decoded T array
     */
    virtual uint32_t Decode(T* dest, uint32_t destLen,
                            common::ByteSliceReader& sliceReader) const = 0;

    virtual uint32_t DecodeMayCopy(T*& dest, uint32_t destLen,
                                   common::ByteSliceReader& sliceReader) const
    { return Decode(dest, destLen, sliceReader); }

private:
    IE_LOG_DECLARE();
};

#define INTENCODER_PTR(type)                                            \
    typedef IntEncoder<uint##type##_t> Int##type##Encoder;              \
    typedef std::tr1::shared_ptr<Int##type##Encoder> Int##type##EncoderPtr;

INTENCODER_PTR(8);
INTENCODER_PTR(16);
INTENCODER_PTR(32);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_INT_ENCODER_H
