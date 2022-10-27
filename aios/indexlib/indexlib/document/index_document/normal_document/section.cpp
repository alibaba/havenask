#include "indexlib/document/index_document/normal_document/section.h"
#include <cassert>

using namespace std;

IE_NAMESPACE_BEGIN(document);

const uint32_t Section::MAX_TOKEN_PER_SECTION = 0xffff;
const uint32_t Section::DEFAULT_TOKEN_NUM = 8;
const uint32_t Section::MAX_SECTION_LENGTH = (1 << 11) - 1;

Token* Section::CreateToken(uint64_t hashKey,
                            pos_t posIncrement, 
                            pospayload_t posPayload)
{
    if (mTokenUsed >= mTokenCapacity)
    {
        if (mTokenUsed == MAX_TOKEN_PER_SECTION) {
            return NULL;
        }

        uint32_t newCapacity = std::min((uint32_t)mTokenCapacity * 2, MAX_TOKEN_PER_SECTION);

        Token *newTokens = IE_POOL_COMPATIBLE_NEW_VECTOR(mPool, Token, newCapacity);
        
        memcpy((void*)newTokens, mTokens, mTokenUsed * sizeof(Token));
        IE_POOL_COMPATIBLE_DELETE_VECTOR(mPool, mTokens, mTokenCapacity);
        mTokens = newTokens;
        mTokenCapacity = newCapacity;
    }
        
    Token* retToken = mTokens + mTokenUsed++;

    retToken->mPosIncrement = posIncrement;
    retToken->mPosPayload = posPayload;
    retToken->mTermKey = hashKey;
    return retToken;
}

void Section::Reset()
{
    mTokenUsed = 0;
    mLength = 0;
    mWeight = 0;
}

bool Section::operator==(const Section& right) const
{
    if (this == &right)
    {
        return true;
    }
    if (
            mSectionId != right.mSectionId ||
            mLength != right.mLength ||
            mWeight != right.mWeight
       )
    {
        return false;
    }

    if ( mTokenUsed != right.mTokenUsed )
        return false;
    for( int i = 0; i < mTokenUsed; ++i )
    {
        if (mTokens[i] != right.mTokens[i])
            return false;
    }
    return true;
}
bool Section::operator!=(const Section& right) const
{
    return !operator==(right);
}

void Section::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(mTokenUsed);
    for (size_t i = 0; i < mTokenUsed; i++) {
        dataBuffer.write(mTokens[i]);
    }
    dataBuffer.write(mSectionId);
    dataBuffer.write(mLength);
    dataBuffer.write(mWeight);
}

void Section::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(mTokenUsed);
    IE_POOL_COMPATIBLE_DELETE_VECTOR(mPool, mTokens, mTokenCapacity);
    mTokens = IE_POOL_COMPATIBLE_NEW_VECTOR(mPool, Token, mTokenUsed);
    for (size_t i = 0; i < mTokenUsed; i++) {
        dataBuffer.read(mTokens[i]);
    }
    dataBuffer.read(mSectionId);
    dataBuffer.read(mLength);
    dataBuffer.read(mWeight);
    mTokenCapacity = mTokenUsed;
}

IE_NAMESPACE_END(document);
