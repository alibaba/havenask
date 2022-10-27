#ifndef __INDEXLIB_HASH_STRING_H
#define __INDEXLIB_HASH_STRING_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include <autil/HashAlgorithm.h>

IE_NAMESPACE_BEGIN(util);

class HashString
{
public:
    static inline void Hash(
            autil::uint128_t& t, const char* str, size_t size)
    {
        t = autil::HashAlgorithm::hashString128(str, size);
    }

    static inline void Hash(
            uint64_t& t, const char* str, size_t size)
    {
        t = autil::HashAlgorithm::hashString64(str, size);
    }

    static inline void Hash(
            uint32_t& t, const char* str, size_t size)
    {
        t = autil::HashAlgorithm::hashString(str, size, 0);
    }

    static inline uint64_t Hash(const char* data, size_t size)
    {
        uint64_t t = autil::HashAlgorithm::hashString(data, size, 0);
        t <<= 32;
        t |= autil::HashAlgorithm::hashString(data, size, 1);
        return t;
    }
};

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_HASH_STRING_H
