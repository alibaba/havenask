#ifndef __INDEXLIB_SECTION_H
#define __INDEXLIB_SECTION_H

#include <vector>
#include <tr1/memory>
#include <autil/DynamicBuf.h>
#include <autil/DataBuffer.h>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/document/index_document/normal_document/token.h"

IE_NAMESPACE_BEGIN(document);

#pragma pack(push, 2)

class Section
{
public:
    static const uint32_t MAX_TOKEN_PER_SECTION;
    static const uint32_t DEFAULT_TOKEN_NUM;
    static const uint32_t MAX_SECTION_LENGTH;
public:
    Section(uint32_t tokenCount = DEFAULT_TOKEN_NUM,
            autil::mem_pool::PoolBase *pool = NULL)
        : mPool(pool)
        , mSectionId(INVALID_SECID)
        , mLength(0)
        , mWeight(0)
        , mTokenUsed(0)
        , mTokenCapacity(std::min(tokenCount, MAX_TOKEN_PER_SECTION))
    {
        if(mTokenCapacity != 0)
        {
            mTokens = IE_POOL_COMPATIBLE_NEW_VECTOR(mPool, Token, mTokenCapacity);
        }
        else
        {
            mTokens = NULL;
        }
    }

    ~Section()
    {
        if(mTokens)
        {
            IE_POOL_COMPATIBLE_DELETE_VECTOR(mPool, mTokens, mTokenCapacity);
            mTokens = NULL;
        }
    }

public:
    //used in build process for reusing token object 
    Token* CreateToken(uint64_t hashKey, 
                       pos_t posIncrement = 0, 
                       pospayload_t posPayload = 0);

    void Reset();

    size_t GetTokenCount() const { return mTokenUsed; }
    
    Token* GetToken(int32_t idx)
    {
        assert(idx < mTokenUsed && idx >= 0);
        return mTokens + idx;
    }

    const Token* GetToken(int32_t idx) const
    {
        assert(idx < mTokenUsed && idx >= 0);
        return mTokens + idx;
    }

    void SetSectionId(sectionid_t sectionId) { mSectionId = sectionId; }
    sectionid_t GetSectionId() const { return mSectionId; }

    section_weight_t GetWeight() const { return mWeight; }
    void SetWeight(section_weight_t weight)  { mWeight = weight; }

    section_len_t GetLength() const { return mLength; }
    void SetLength(section_len_t length) { mLength = length; }

    bool operator==(const Section& right) const;
    bool operator!=(const Section& right) const;

    void SetTokenUsed(int32_t tokenUsed)
    {
        mTokenUsed  = tokenUsed;
    }

    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);

private:
    // for test
    section_len_t GetCapacity() const {
        return mTokenCapacity;
    }

private:
    autil::mem_pool::PoolBase* mPool;
    Token* mTokens;

    sectionid_t mSectionId;
    section_len_t mLength;
    section_weight_t mWeight;
    section_len_t mTokenUsed;
    section_len_t mTokenCapacity;
    friend class SectionTest;
};

typedef std::tr1::shared_ptr<Section> SectionPtr;

#pragma pack(pop)

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_SECTION_H

