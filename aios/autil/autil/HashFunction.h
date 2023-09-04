/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <stdint.h>
#include <string.h>

namespace autil {

/**
 *hash function StoreBkdrHash() for char* ending with '\0'
 *@param szStr
 *@return uint64_t hash value
 */
inline uint64_t storeBkdrHash(const char *str) {
    uint64_t seed = 131; // 31 131 1313 13131 131313 etc..
    uint64_t hash = 0;
    const unsigned char *p;
    p = reinterpret_cast<const unsigned char *>(str);
    for (; *p;) {
        hash = hash * seed + (*p++);
    }
    return hash;
}

/**
 *hash function StoreBkdrHash() for const char* with given length
 *@param szStr
 *@param len the length of szStr
 *@return uint64_t hash value
 */
inline uint64_t storeBkdrHash(const char *str, int32_t len) {
    const uint64_t seed = 131; // 31 131 1313 13131 131313 etc..
    uint64_t hash = 0;
    const unsigned char *p;
    p = reinterpret_cast<const unsigned char *>(str);
    for (int32_t i = 0; i < len; ++i) {
        hash = hash * seed + (*p++);
    }
    return hash;
}

/****************************************************************************/

/**
 *ELF hash function
 */
inline uint64_t storeElfHash(const char *str) {
    uint64_t hash = 0;
    uint64_t x = 0;
    const unsigned char *p;
    p = reinterpret_cast<const unsigned char *>(str);
    while (*p) {
        hash = (hash << 4) + (*p++);
        if ((x = hash & __UINT64_C(0xF000000000000000)) != 0) {
            hash ^= (x >> 56);
            hash &= ~x;
        }
    }
    return hash;
}

inline uint64_t storeElfHash(const char *str, int32_t len) {
    uint64_t hash = 0;
    uint64_t x = 0;
    const unsigned char *p;
    p = reinterpret_cast<const unsigned char *>(str);
    for (int32_t i = 0; i < len; ++i) {
        hash = (hash << 4) + (*p++);
        if ((x = hash & __UINT64_C(0xF000000000000000)) != 0) {
            hash ^= (x >> 56);
            hash &= ~x;
        }
    }
    return hash;
}

/****************************************************************************/

/**
 *RS Hash Function
 */
inline uint64_t storeRsHash(const char *str) {
    uint64_t b = 378551;
    uint64_t a = 63689;
    uint64_t hash = 0;
    const unsigned char *p;
    p = reinterpret_cast<const unsigned char *>(str);
    while (*p) {
        hash = hash * a + (*p++);
        a *= b;
    }
    return hash;
}

inline uint64_t storeRsHash(const char *str, int32_t len) {
    uint64_t b = 378551;
    uint64_t a = 63689;
    uint64_t hash = 0;
    const unsigned char *p;
    p = reinterpret_cast<const unsigned char *>(str);
    for (int32_t i = 0; i < len; ++i) {
        hash = hash * a + (*p++);
        a *= b;
    }
    return hash;
}

/****************************************************************************/

/**
 *JS Hash Function
 */
inline uint64_t storeJsHash(const char *str) {
    uint64_t hash = 1315423911;
    const unsigned char *p;
    p = reinterpret_cast<const unsigned char *>(str);
    while (*p) {
        hash ^= ((hash << 5) + (*p++) + (hash >> 2));
    }
    return hash;
}

inline uint64_t storeJsHash(const char *str, int32_t len) {
    uint64_t hash = 1315423911;
    const unsigned char *p;
    p = reinterpret_cast<const unsigned char *>(str);
    for (int32_t i = 0; i < len; ++i) {
        hash ^= ((hash << 5) + (*p++) + (hash >> 2));
    }
    return hash;
}

/****************************************************************************/

/**
 *SDBM Hash Function
 */
inline uint64_t storeSdbmHash(const char *str) {
    uint64_t hash = 0;
    const unsigned char *p;
    p = reinterpret_cast<const unsigned char *>(str);
    while (*p) {
        hash = (*p++) + (hash << 6) + (hash << 16) - hash;
    }
    return hash;
}

inline uint64_t storeSdbmHash(const char *str, int32_t len) {
    uint64_t hash = 0;
    const unsigned char *p;
    p = reinterpret_cast<const unsigned char *>(str);
    for (int32_t i = 0; i < len; ++i) {
        hash = (*p++) + (hash << 6) + (hash << 16) - hash;
    }
    return hash;
}

/****************************************************************************/

/**
 *PJW Hash Function
 */
inline uint64_t storePjwHash(const char *str) {
    uint32_t BitsInUnsignedInt = (uint32_t)(sizeof(uint32_t) * 8);
    uint32_t ThreeQuarters = (uint32_t)((BitsInUnsignedInt * 3) / 4);
    uint32_t OneEighth = (uint32_t)(BitsInUnsignedInt / 8);
    uint32_t HighBits = (uint32_t)(0xFFFFFFFF) << (BitsInUnsignedInt - OneEighth);
    uint32_t hash = 0;
    uint32_t test = 0;

    const unsigned char *p;
    p = reinterpret_cast<const unsigned char *>(str);
    while (*p) {
        hash = (hash << OneEighth) + (*p++);
        if ((test = hash & HighBits) != 0) {
            hash = ((hash ^ (test >> ThreeQuarters)) & (~HighBits));
        }
    }
    return (uint64_t)hash;
}

inline uint64_t storePjwHash(const char *str, int32_t len) {
    uint32_t BitsInUnsignedInt = (uint32_t)(sizeof(uint32_t) * 8);
    uint32_t ThreeQuarters = (uint32_t)((BitsInUnsignedInt * 3) / 4);
    uint32_t OneEighth = (uint32_t)(BitsInUnsignedInt / 8);
    uint32_t HighBits = (uint32_t)(0xFFFFFFFF) << (BitsInUnsignedInt - OneEighth);
    uint32_t hash = 0;
    uint32_t test = 0;

    for (int32_t i = 0; i < len; ++i) {
        hash = (hash << OneEighth) + str[i];

        if ((test = hash & HighBits) != 0) {
            hash = ((hash ^ (test >> ThreeQuarters)) & (~HighBits));
        }
    }
    return (uint64_t)hash;
}

/****************************************************************************/

/**
 *DJB Hash Function
 */
inline uint64_t storeDjbHash(const char *str) {
    uint32_t hash = 5381;
    const unsigned char *p;
    p = reinterpret_cast<const unsigned char *>(str);
    while (*p) {
        hash = ((hash << 5) + hash) + (*p++);
    }
    return (uint64_t)hash;
}

inline uint64_t storeDjbHash(const char *str, int32_t len) {
    uint32_t hash = 5381;
    for (int32_t i = 0; i < len; ++i) {
        hash = ((hash << 5) + hash) + str[i];
    }
    return (uint64_t)hash;
}

/****************************************************************************/

/**
 *DEK Hash Function
 */
inline uint64_t storeDekHash(const char *str) {
    uint32_t hash = strlen(str);
    const unsigned char *p;
    p = reinterpret_cast<const unsigned char *>(str);
    while (*p) {
        hash = ((hash << 5) ^ (hash >> 27)) ^ (*p++);
    }
    return (uint64_t)hash;
}

inline uint64_t storeDekHash(const char *str, int32_t len) {
    uint32_t hash = static_cast<uint32_t>(len);
    for (int32_t i = 0; i < len; ++i) {
        hash = ((hash << 5) ^ (hash >> 27)) ^ str[i];
    }
    return (uint64_t)hash;
}

/****************************************************************************/

/**
 *BP Hash Function
 */
inline uint64_t storeBpHash(const char *str) {
    uint32_t hash = 0;
    const unsigned char *p;
    p = reinterpret_cast<const unsigned char *>(str);
    while (*p) {
        hash = hash << 7 ^ (*p++);
    }
    return (uint64_t)hash;
}

inline uint64_t storeBpHash(const char *str, int32_t len) {
    uint32_t hash = 0;
    for (int32_t i = 0; i < len; ++i) {
        hash = hash << 7 ^ str[i];
    }
    return (uint64_t)hash;
}

/****************************************************************************/

/**
 *FNV Hash Function
 */
inline uint64_t storeFnvHash(const char *str) {
    const uint32_t fnv_prime = 0x811C9DC5;
    uint32_t hash = 0;
    const unsigned char *p;
    p = reinterpret_cast<const unsigned char *>(str);
    while (*p) {
        hash *= fnv_prime;
        hash ^= (*p++);
    }

    return (uint64_t)hash;
}

inline uint64_t storeFnvHash(const char *str, int32_t len) {
    const uint32_t fnv_prime = 0x811C9DC5;
    uint32_t hash = 0;
    for (int32_t i = 0; i < len; ++i) {
        hash *= fnv_prime;
        hash ^= str[i];
    }

    return (uint64_t)hash;
}

/****************************************************************************/

/**
 *AP Hash Function
 */
inline uint64_t storeApHash(const char *str) {
    uint32_t hash = 0xAAAAAAAA;

    const unsigned char *p;
    p = reinterpret_cast<const unsigned char *>(str);
    uint32_t i = 0;
    while (*p) {
        hash ^= ((i & 1) == 0) ? ((hash << 7) ^ (*p) * (hash >> 3)) : (~((hash << 11) + ((*p++) ^ (hash >> 5))));
        ++i;
    }
    return (uint64_t)hash;
}

inline uint64_t storeApHash(const char *str, int32_t len) {
    uint32_t hash = 0xAAAAAAAA;

    for (int32_t i = 0; i < len; ++i) {
        hash ^= ((i & 1) == 0) ? ((hash << 7) ^ str[i] * (hash >> 3)) : (~((hash << 11) + (str[i] ^ (hash >> 5))));
    }
    return (uint64_t)hash;
}

/****************************************************************************/

/**
 * Unique Hash Function
 */
inline uint64_t storeUniqueHash(const char *str) {
    uint64_t hash = storeElfHash(str);
    uint64_t hash2 = storeRsHash(str);
    hash = hash << 32 | (hash2 & 0xFFFFFFFFUL);
    return hash;
}

inline uint64_t storeUniqueHash(const char *str, int32_t len) {
    uint64_t hash = storeElfHash(str, len);
    uint64_t hash2 = storeRsHash(str, len);
    hash = hash << 32 | (hash2 & 0xFFFFFFFFUL);
    return hash;
}

/**
 *Hash Function for check
 */
inline uint64_t storeCheckHash(const char *str) {
    uint64_t hash = storeJsHash(str);
    uint64_t hash2 = storeSdbmHash(str);
    hash = hash << 32 | (hash2 & 0xFFFFFFFFUL);
    return hash;
}

inline uint64_t storeCheckHash(const char *str, int32_t len) {
    uint64_t hash = storeJsHash(str, len);
    uint64_t hash2 = storeSdbmHash(str, len);
    hash = hash << 32 | (hash2 & 0xFFFFFFFFUL);
    return hash;
}

} // namespace autil
