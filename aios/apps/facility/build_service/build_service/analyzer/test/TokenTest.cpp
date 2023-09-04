#include "build_service/analyzer/Token.h"

#include "autil/DataBuffer.h"
#include "build_service/test/unittest.h"

namespace build_service { namespace analyzer {

class TokenTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void TokenTest::setUp() {}

void TokenTest::tearDown() {}

TEST_F(TokenTest, testSimpleProcess)
{
    BS_LOG(DEBUG, "Begin Test!");

    Token token;
    token.setText("aa");
    ASSERT_EQ(std::string("aa"), token.getText());
}

TEST_F(TokenTest, testGetHashKey)
{
    BS_LOG(DEBUG, "Begin Test!");

    uint64_t hash1 = Token::getHashKey("a");
    uint64_t hash2 = Token::getHashKey("b");
    ASSERT_TRUE(hash1 != hash2);

    uint64_t hash3 = Token::getHashKey((char*)NULL);
    ASSERT_EQ((uint64_t)0, hash3);

    uint64_t hash4 = Token::getHashKey("");
    ASSERT_TRUE(hash3 != hash4);

    Token token;
    uint64_t hash5 = token.getHashKey();
    ASSERT_TRUE(hash5 == hash4);

    token.setNormalizedText("a");
    uint64_t hash6 = token.getHashKey();
    ASSERT_TRUE(hash6 == hash1);
}

TEST_F(TokenTest, testGetHashKeyWithNullStr)
{
    BS_LOG(DEBUG, "Begin Test!");

    uint64_t hash1 = Token::getHashKey((char*)NULL);
    ASSERT_EQ((uint64_t)0, hash1);

    uint64_t hash2 = Token::getHashKey("");
    ASSERT_TRUE(hash1 != hash2);
}

TEST_F(TokenTest, testSerialize)
{
    autil::DataBuffer buf1;
    Token token1("abc", 0);
    token1.serialize(buf1);
    Token deserializedToken1;
    deserializedToken1.deserialize(buf1);
    ASSERT_TRUE(token1 == deserializedToken1);
    deserializedToken1.setIsRetrieve(false);
    ASSERT_TRUE(!(token1 == deserializedToken1));

    autil::DataBuffer buf2;
    Token token2("ABC", 0, "abc", true);
    token2.setIsStopWord(true);
    token2.setIsBasicRetrieve(true);
    token2.serialize(buf2);
    Token deserializedToken2;
    deserializedToken2.deserialize(buf2);
    ASSERT_TRUE(token2 == deserializedToken2);
}

}} // namespace build_service::analyzer
