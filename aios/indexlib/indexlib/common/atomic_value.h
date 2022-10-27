#ifndef __INDEXLIB_ATOMIC_VALUE_H
#define __INDEXLIB_ATOMIC_VALUE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/byte_slice_writer.h"
#include "indexlib/common/byte_slice_reader.h"

IE_NAMESPACE_BEGIN(common);

class AtomicValue
{
public:
    enum ValueType
    {
        vt_unknown,
        vt_uint8,
        vt_uint16,
        vt_uint32,
        vt_int8,
        vt_int16,
        vt_int32,
    };

    AtomicValue()
    {
        mLocation = 0;
        mOffset = 0;
    }
    virtual ~AtomicValue() {}

public:
    virtual ValueType GetType() const = 0;
    virtual size_t GetSize() const = 0;

    uint32_t GetLocation() const { return mLocation; }
    void SetLocation(const uint32_t& location)  
    { mLocation = location; }

    uint32_t GetOffset() const { return mOffset; }
    void SetOffset(const uint32_t& offset)  { mOffset = offset; }

    virtual uint32_t Encode(uint8_t mode, ByteSliceWriter& sliceWriter,
                            const uint8_t* src, uint32_t len) const = 0;

    virtual uint32_t Decode(uint8_t mode, uint8_t* dest, uint32_t destLen,
                            common::ByteSliceReader& sliceReader) const = 0;
private:
    uint32_t mLocation;
    uint32_t mOffset;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AtomicValue);

typedef std::vector<AtomicValue*> AtomicValueVector;

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_ATOMIC_VALUE_H
