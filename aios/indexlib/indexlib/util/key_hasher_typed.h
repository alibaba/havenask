#ifndef __INDEXLIB_KEY_HASHER_TYPED_H
#define __INDEXLIB_KEY_HASHER_TYPED_H

#include <tr1/memory>
#include <limits.h>
#include <autil/StringUtil.h>
#include <autil/HashAlgorithm.h>
#include <autil/MurmurHash.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/term.h"

IE_NAMESPACE_BEGIN(util);

#define DECLARE_HASHER_IDENTIFIER(id)                           \
    static std::string Identifier() { return std::string(#id);} \
    std::string GetIdentifier() const override { return Identifier();}

class KeyHasher 
{
public:
    KeyHasher(uint16_t salt = 0)
        : mSalt(salt)
    {}
    virtual ~KeyHasher() {}

public:
    virtual bool GetHashKey(const char* word, dictkey_t& key) const = 0;
    virtual bool GetHashKey(const char* word, size_t size, dictkey_t& key) const = 0;
    virtual std::string GetIdentifier() const = 0;
    bool GetHashKey(const util::Term& term, dictkey_t& key) const
    {
        if (term.GetTermHashKey(key))
        {
            return true;
        }
        if (GetHashKey(term.GetWord().c_str(),
                       term.GetWord().size(), key))
        {
            const_cast<util::Term&>(term).SetTermHashKey(key);
            return true;
        }
        return false;
    }
    bool GetHashKey(const char* word, size_t size, autil::uint128_t& key)
    {
        key = autil::HashAlgorithm::hashString128(word, size);
        return true;
    }

    virtual bool GetHashKey(const autil::ConstString& key, dictkey_t& hashKey) const
    {
        hashKey = autil::HashAlgorithm::hashString64(key.data(), key.size());
        return true;
    }    
    virtual bool IsOriginalKeyValid(
            const char* word, size_t size, bool hash64) const = 0;

protected:
    uint16_t mSalt;

};

DEFINE_SHARED_PTR(KeyHasher);

/////////////////////////////TextHasher///////////////////////////////

class TextHasher final : public KeyHasher
{
public:
    TextHasher() {}
    ~TextHasher() {}

public:
    bool GetHashKey(const char* word, dictkey_t& key) const override final
    {
        key = autil::HashAlgorithm::hashString64(word);
        return true;
    }
    bool GetHashKey(const char* word, size_t size, dictkey_t& key) const override final
    {
        key = autil::HashAlgorithm::hashString64(word, size);
        return true;
    }
    bool IsOriginalKeyValid(const char* word, size_t size, bool hash64) const override final
    {
        return true;
    }

public:
    DECLARE_HASHER_IDENTIFIER(StringHash64)
};
typedef TextHasher DefaultHasher;

/////////////////////////////MurmurHasher///////////////////////////////

class MurmurHasher final : public KeyHasher
{
public:
    MurmurHasher(uint16_t salt = NO_SALT)
        : KeyHasher(salt) {}
    ~MurmurHasher() {}

private:
    static const uint64_t SEED = 545609244;
    
public:
    static const uint16_t NO_SALT = 0xffff;    

public:
    bool GetHashKey(const char* word, dictkey_t& key) const override final
    {
        size_t size = strlen(word);
        key = InnerGetHashKey(word, size, mSalt);
        return true;
    }
    bool GetHashKey(const char* word, size_t size, dictkey_t& key) const override final
    {
        key = InnerGetHashKey(word, size, mSalt);        
        return true;
    }
    bool IsOriginalKeyValid(const char* word, size_t size, bool hash64) const override final
    {
        return true;
    }

public:
    static bool DoGetHashKey(const autil::ConstString& key, dictkey_t& hashKey, uint16_t salt = NO_SALT)
    {
        hashKey = InnerGetHashKey(key.data(), key.size(), salt);
        return true;
    }

    bool GetHashKey(const autil::ConstString& key, dictkey_t& hashKey) const override final
    {
        hashKey = InnerGetHashKey(key.data(), key.size(), mSalt);
        return true;
    }

private:
    static uint64_t InnerGetHashKey(const void* key, int len, uint64_t salt)
    {
        if (salt == NO_SALT)
        {
            return autil::MurmurHash::MurmurHash64A(key, len, SEED);
        }
        return autil::MurmurHash::MurmurHash64A(key, len, SEED, salt);
    }
    
public:
    DECLARE_HASHER_IDENTIFIER(MurmurHash)
};

/////////////////////////////NumberHasher///////////////////////////////

template <typename KeyType, bool negative>
class NumberHasherTyped final : public KeyHasher
{
public:
    NumberHasherTyped(uint16_t salt = 0)
        : KeyHasher(salt) {}
    ~NumberHasherTyped() {}

public:
    bool GetHashKey(const char* word, dictkey_t& key) const override final
    {
        return InnerGetHashKey(word, key, mSalt);
    }
    bool GetHashKey(const char* word, size_t size, dictkey_t& key) const override final
    {
        (void)size;
        return InnerGetHashKey(word, key, mSalt);
    }
    bool IsOriginalKeyValid(const char* word, size_t size, bool hash64) const override final
    {
        if (!hash64)
        {
            return true;
        }
        dictkey_t key;
        return GetHashKey(word, key);
    }

public:
    // TODO : merge with DoGetHashKey with salt
    static bool DoGetHashKey(const autil::ConstString& key, dictkey_t& hashKey)
    {
        return InnerGetHashKey(key.data(), hashKey, 0);
    }

    static bool DoGetHashKey(const autil::ConstString& key, dictkey_t& hashKey, uint16_t salt)
    {
        return InnerGetHashKey(key.data(), hashKey, salt);
    }    
    
    bool GetHashKey(const autil::ConstString& key, dictkey_t& hashKey) const override final
    {
        return InnerGetHashKey(key.data(), hashKey, mSalt);
    }
private:
    static bool InnerGetHashKey(const char* word, dictkey_t& key, uint16_t salt);

public:
    DECLARE_HASHER_IDENTIFIER(Number)
};

typedef NumberHasherTyped<uint8_t, true> Int8NumberHasher;
typedef NumberHasherTyped<uint8_t, false> UInt8NumberHasher;
typedef NumberHasherTyped<uint16_t, true> Int16NumberHasher;
typedef NumberHasherTyped<uint16_t, false> UInt16NumberHasher;
typedef NumberHasherTyped<uint32_t, true> Int32NumberHasher;
typedef NumberHasherTyped<uint32_t, false> UInt32NumberHasher;
typedef NumberHasherTyped<uint64_t, true> Int64NumberHasher;
typedef NumberHasherTyped<uint64_t, false> UInt64NumberHasher;


inline bool ConstStringToUInt64(const autil::ConstString& key, uint64_t& hashKey)
{
    const char *str = key.data();
    size_t size = key.size();
    if (NULL == str || *str == 0)
    {
        return false;
    }

    for (; str && size && isspace(*str); str++, size--) ;
    if (*str == '+')
    {
        ++str;
        --size;
    }
    if (size == 0)
    {
        return false;
    }

    hashKey = 0;
    while(isdigit(*str) && size)
    {
        uint64_t result = hashKey * 10 + (*str - '0');
        if (hashKey > result)
        {
            return false;
        }
        hashKey = result;
        ++str;
        --size;
    }
    if (size > 0)
    {
        return *str == 0;
    }
    return true;
}

inline bool ConstStringToInt64(const autil::ConstString& key, uint64_t& hashKey)
{
    const char *str = key.data();
    size_t size = key.size();
    if (NULL == str || *str == 0)
    {
        return false;
    }

    for (; str && size && isspace(*str); str++, size--) ;
    bool negative = false;
    if (*str == '-')
    {
        negative = true;
        ++str;
        --size;
    }
    else if(*str == '+')
    {
        ++str;
        --size;
    }
    if (size == 0)
    {
        return false;
    }

    uint64_t cutoff = negative ? -(uint64_t)LONG_MIN : LONG_MAX;
    hashKey = 0;
    while(isdigit(*str) && size)
    {
        uint64_t result = hashKey * 10 + (*str - '0');
        if (result > cutoff || result < hashKey)
        {
            return false;
        }
        hashKey = result;
        ++str;
        --size;
    }
    if (negative)
    {
        hashKey = (uint64_t)(-hashKey);
    }
    return (size > 0) ? (*str == 0) : true;
}

#define GETHASHKEY(BITS)                                                  \
    template<>                                                            \
    inline bool NumberHasherTyped<uint##BITS##_t, false>::InnerGetHashKey(\
            const char *word, dictkey_t &key, uint16_t salt)            \
    {                                                                   \
        uint##BITS##_t tempKey;                                         \
        bool ret = autil::StringUtil::strToUInt##BITS(word, tempKey);   \
        key = ((dictkey_t)salt << 48) | (dictkey_t)tempKey;             \
        return ret;                                                     \
    }                                                                   \
                                                                        \
    template<>                                                          \
    inline bool NumberHasherTyped<uint##BITS##_t, true>::InnerGetHashKey(  \
            const char *word, dictkey_t &key, uint16_t salt)            \
    {                                                                   \
        int##BITS##_t tempKey;                                          \
        bool ret = autil::StringUtil::strToInt##BITS(word, tempKey);    \
        key = ((dictkey_t)salt << 48) | (dictkey_t)tempKey;             \
        return ret;                                                     \
    }

GETHASHKEY(8)
GETHASHKEY(16)
GETHASHKEY(32)
GETHASHKEY(64)

#undef GETHASHKEY
#undef DECLARE_HASHER_IDENTIFIER

inline bool GetHashKey(HashFunctionType hasherType,
                       const autil::ConstString& key, dictkey_t& hashKey)
{
    switch(hasherType)
    {
    case hft_uint64:
        return ConstStringToUInt64(key, hashKey);
        //return UInt64NumberHasher::DoGetHashKey(key, hashKey);
    case hft_int64:
        return ConstStringToInt64(key, hashKey);
        //return Int64NumberHasher::DoGetHashKey(key, hashKey);
    case hft_murmur:
        return MurmurHasher::DoGetHashKey(key, hashKey);
    default:
        return false;
    }
    return false;
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_KEY_HASHER_TYPED_H
