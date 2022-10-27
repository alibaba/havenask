#ifndef __INDEXLIB_EQUIVALENT_COMPRESS_TRAITS_H
#define __INDEXLIB_EQUIVALENT_COMPRESS_TRAITS_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(common);

template <typename T>
struct EquivalentCompressTraits
{
    typedef T EncoderType;
    class ZigZagEncoder
    {
    public:
        static inline EncoderType Encode(T value) __ALWAYS_INLINE
        {
            return value;
        }
        static inline T Decode(EncoderType value) __ALWAYS_INLINE
        {
            return value;
        }
    };
};

#define DECLARE_UNION_COMPRESS_TRAITS(TYPE, ENCODETYPE)                 \
    template <>                                                         \
    struct EquivalentCompressTraits<TYPE>                               \
    {                                                                   \
        typedef ENCODETYPE EncoderType;                                 \
        union UnionValue                                                \
        {                                                               \
            TYPE oValue;                                                \
            EncoderType eValue;                                         \
        };                                                              \
        class ZigZagEncoder                                             \
        {                                                               \
        public:                                                         \
            static inline EncoderType Encode(TYPE value) __ALWAYS_INLINE \
            {                                                           \
                UnionValue uv;                                          \
                uv.oValue = value;                                      \
                return uv.eValue;                                       \
            }                                                           \
            static inline TYPE Decode(EncoderType value) __ALWAYS_INLINE \
            {                                                           \
                UnionValue uv;                                          \
                uv.eValue = value;                                      \
                return uv.oValue;                                       \
            }                                                           \
        };                                                              \
    };                                                                  \

DECLARE_UNION_COMPRESS_TRAITS(float,  uint32_t)
DECLARE_UNION_COMPRESS_TRAITS(double, uint64_t)

#define DECLARE_COMPRESS_TRAITS(TYPE, ENCODETYPE, BITNUM)               \
    template <>                                                         \
    struct EquivalentCompressTraits<TYPE>                               \
    {                                                                   \
        typedef ENCODETYPE EncoderType;                                 \
        class ZigZagEncoder                                             \
        {                                                               \
        public:                                                         \
            static inline EncoderType Encode(TYPE value) __ALWAYS_INLINE \
            {                                                           \
                return (value << 1) ^ (value >> BITNUM);                \
            }                                                           \
            static inline TYPE Decode(EncoderType value) __ALWAYS_INLINE \
            {                                                           \
                return (value >> 1) ^ -(TYPE)(value & 1);               \
            }                                                           \
        };                                                              \
    };                                                                  \

DECLARE_COMPRESS_TRAITS(int8_t,  uint8_t,  7)
DECLARE_COMPRESS_TRAITS(int16_t, uint16_t, 15)
DECLARE_COMPRESS_TRAITS(int32_t, uint32_t, 31)
DECLARE_COMPRESS_TRAITS(int64_t, uint64_t, 63)

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_EQUIVALENT_COMPRESS_TRAITS_H
