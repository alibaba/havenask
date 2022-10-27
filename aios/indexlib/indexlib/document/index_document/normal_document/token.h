#ifndef __INDEXLIB_WP_TOKEN_H
#define __INDEXLIB_WP_TOKEN_H

#include <tr1/memory>
#include <autil/HashAlgorithm.h>
#include <autil/ConstString.h>
#include <autil/DataBuffer.h>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

IE_NAMESPACE_BEGIN(document);

#pragma pack(push, 1)

class Token
{
public:
    static const int TOKEN_SERIALIZE_MAGIC;
    static const std::string DUMMY_TERM_STRING;

private:    
    Token()
        : mTermKey(0)
        , mPosIncrement(0)
        , mPosPayload(0)
    {}

    ~Token()
    {}

public:
    uint64_t GetHashKey() const
    {
        return mTermKey;
    }

    void SetPosIncrement(pos_t posInc) 
    { 
        mPosIncrement = posInc; 
    }

    pos_t GetPosIncrement() const 
    { 
        return mPosIncrement;
    }

    void SetPosPayload(pospayload_t payload) 
    { 
        mPosPayload = payload; 
    }
    
    pospayload_t GetPosPayload() const 
    { 
        return mPosPayload; 
    }

    bool operator == (const Token& right) const;
    bool operator != (const Token& right) const;

    static void CreatePairToken(const Token& firstToken, const Token& nextToken,
                                Token& pairToken);

    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
private:
    uint64_t            mTermKey;           // 8 byte
    pos_t               mPosIncrement;      // 4 byte
    pospayload_t        mPosPayload;        // 1 byte

private:
    friend class TokenTest;
    friend class Section;
};

#pragma pack(pop)

DEFINE_SHARED_PTR(Token);
IE_NAMESPACE_END(document);

#endif //__INDEXLIB_WP_TOKEN_H
