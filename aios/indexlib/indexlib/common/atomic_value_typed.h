#ifndef __INDEXLIB_ATOMIC_VALUE_TYPED_H
#define __INDEXLIB_ATOMIC_VALUE_TYPED_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/atomic_value.h"
#include "indexlib/common/numeric_compress/encoder_provider.h"

IE_NAMESPACE_BEGIN(common);

template<typename T>
struct ValueTypeTraits
{
    static const AtomicValue::ValueType TYPE = AtomicValue::vt_unknown;
};

#define FORMAT_TYPE_TRAITS_HELPER(x, y)                 \
    template<>                                          \
    struct ValueTypeTraits<x>                           \
    {                                                   \
        static const AtomicValue::ValueType TYPE = y;   \
    }

FORMAT_TYPE_TRAITS_HELPER(int8_t, AtomicValue::vt_int8);
FORMAT_TYPE_TRAITS_HELPER(int16_t, AtomicValue::vt_int16);
FORMAT_TYPE_TRAITS_HELPER(int32_t, AtomicValue::vt_int32);
FORMAT_TYPE_TRAITS_HELPER(uint8_t, AtomicValue::vt_uint8);
FORMAT_TYPE_TRAITS_HELPER(uint16_t, AtomicValue::vt_uint16);
FORMAT_TYPE_TRAITS_HELPER(uint32_t, AtomicValue::vt_uint32);

template<typename T>
struct EncoderTypeTraits
{
    struct UnknownType {};
    typedef UnknownType Encoder;
};

#define ENCODER_TYPE_TRAITS_HELPER(type, encoder, data_type)    \
    template<>                                                  \
    struct EncoderTypeTraits<type>                              \
    {                                                           \
        typedef encoder Encoder;                                \
        typedef data_type DataType;                             \
    };

ENCODER_TYPE_TRAITS_HELPER(int8_t, Int8Encoder, uint8_t);
ENCODER_TYPE_TRAITS_HELPER(uint8_t, Int8Encoder, uint8_t);
ENCODER_TYPE_TRAITS_HELPER(int16_t, Int16Encoder, uint16_t);
ENCODER_TYPE_TRAITS_HELPER(uint16_t, Int16Encoder, uint16_t);
ENCODER_TYPE_TRAITS_HELPER(int32_t, Int32Encoder, uint32_t);
ENCODER_TYPE_TRAITS_HELPER(uint32_t, Int32Encoder, uint32_t);

template <typename T>
class AtomicValueTyped : public AtomicValue
{
public:
    typedef typename EncoderTypeTraits<T>::Encoder Encoder;
    typedef typename EncoderTypeTraits<T>::DataType DataType;

public:
    void SetValue(T value) { mValue = value; }
    T GetValue() const { return mValue; }

    virtual ValueType GetType() const
    { return ValueTypeTraits<T>::TYPE; }

    virtual size_t GetSize() const
    { return sizeof(T); }

    void SetEncoder(uint8_t mode, const Encoder* encoder)
    {
        assert(mode < 4);
        mEncoders[mode] = encoder;
    }
    virtual uint32_t Encode(uint8_t mode, ByteSliceWriter& sliceWriter,
                            const uint8_t* src, uint32_t len) const
    {
        assert(mode < 4);
        assert(mEncoders[mode]);
        return mEncoders[mode]->Encode(sliceWriter, (const DataType*)src, 
                len / sizeof(T));
    }

    virtual uint32_t Decode(uint8_t mode, uint8_t* dest, uint32_t destLen,
                            ByteSliceReader& sliceReader) const
    {
        assert(mode < 4);
        assert(mEncoders[mode]);
        return mEncoders[mode]->Decode((DataType*)dest, destLen / sizeof(T), 
                sliceReader);
    }

private:
    T mValue;
    //TODO: use CompressMode in index_define.h
    //TODO: set encoder pointer NULL
    const Encoder* mEncoders[4];
    
private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_ATOMIC_VALUE_TYPED_H
