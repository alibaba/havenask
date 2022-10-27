#include "indexlib/document/index_document/normal_document/token.h"
#include <cassert>

using namespace std;
IE_NAMESPACE_BEGIN(document);

const string Token::DUMMY_TERM_STRING = "#$%^&*()_+!@~|\\";

bool Token::operator==(const Token& right) const
{
    if (this == &right)
    {
        return true;
    }
    
    return mTermKey == right.mTermKey
        && mPosIncrement == right.mPosIncrement
        && mPosPayload == right.mPosPayload;
}

bool Token::operator!=(const Token& right) const
{
    return !operator==(right);
}

void Token::CreatePairToken(const Token& firstToken, const Token& nextToken,
                            Token& pairToken)
{
    assert(0);
    // TODO
}

void Token::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(mTermKey);
    dataBuffer.write(mPosIncrement);
    dataBuffer.write(mPosPayload);

}

void Token::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(mTermKey);
    dataBuffer.read(mPosIncrement);
    dataBuffer.read(mPosPayload);
}

IE_NAMESPACE_END(document);
