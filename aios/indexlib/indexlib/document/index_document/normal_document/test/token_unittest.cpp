#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/document/index_document/normal_document/token.h"
#include "indexlib/document/index_document/normal_document/section.h"
#include "indexlib/util/key_hasher_typed.h"
#include <vector>
#include <string>
#include <limits>

using namespace std;

#define NUM_TOKENS 1000
IE_NAMESPACE_USE(index);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(document);
class TokenTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(TokenTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForEqual()
    {
        Section section1, section2;
        GenerateTokens(NUM_TOKENS, section1);
        GenerateTokens(NUM_TOKENS, section2);
        for(int i = 0; i < NUM_TOKENS; ++i)
        {
            for(int j = i; j < NUM_TOKENS; ++j)
            {
                if (i == j)
                    INDEXLIB_TEST_TRUE(*(section1.GetToken(i)) == *(section2.GetToken(j)));
                else
                    INDEXLIB_TEST_TRUE(*(section1.GetToken(i)) != *(section2.GetToken(j)));
            }
        }
    }

    void TestCaseSerializeToken()
    {
        autil::DataBuffer dataBuffer;
        {
            Section section;
            Token *token1 = section.CreateToken(
                    std::numeric_limits<uint64_t>::min(), 
                    std::numeric_limits<pos_t>::min(), 
                    std::numeric_limits<pospayload_t>::min());
            Token *token2 = section.CreateToken(2, 2, 2);
            dataBuffer.write(*token1);
            dataBuffer.write(*token2);
        }
        Token token;

        dataBuffer.read(token);
        INDEXLIB_TEST_EQUAL(std::numeric_limits<uint64_t>::min(), token.GetHashKey());
        INDEXLIB_TEST_EQUAL(std::numeric_limits<pos_t>::min(), token.GetPosIncrement());
        INDEXLIB_TEST_EQUAL(std::numeric_limits<pospayload_t>::min(), token.GetPosPayload());

        dataBuffer.read(token);
        INDEXLIB_TEST_EQUAL((uint64_t)2, token.GetHashKey());
        INDEXLIB_TEST_EQUAL((pos_t)2, token.GetPosIncrement());
        INDEXLIB_TEST_EQUAL((pospayload_t)2, token.GetPosPayload());
    }

private:
    void GenerateTokens(int num, Section& section)
    {
        for(int i = 0; i < num; ++i)
        {
            stringstream ss;
            ss << i ;
            DefaultHasher hasher;
            uint64_t hashKey;
            hasher.GetHashKey(ss.str().c_str(), hashKey);
            section.CreateToken(hashKey, i, i%256);
        }
    }
};

INDEXLIB_UNIT_TEST_CASE(TokenTest, TestCaseForEqual);
INDEXLIB_UNIT_TEST_CASE(TokenTest, TestCaseSerializeToken);

IE_NAMESPACE_END(document);
