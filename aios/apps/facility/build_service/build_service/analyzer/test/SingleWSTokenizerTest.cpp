#include "build_service/analyzer/SingleWSTokenizer.h"

#include "build_service/test/unittest.h"

using namespace std;
using namespace autil;

using namespace build_service::util;

namespace build_service { namespace analyzer {

class SingleWSTokenizerTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void SingleWSTokenizerTest::setUp() {}

void SingleWSTokenizerTest::tearDown() {}

TEST_F(SingleWSTokenizerTest, testSimpleProcess)
{
    BS_LOG(DEBUG, "Begin Test!");
    SingleWSTokenizer tokenizer;
    string str("test \xf5tokenizer");
    tokenizer.tokenize(str.data(), str.size());

    Token token;
    ASSERT_TRUE(tokenizer.next(token));
    ASSERT_EQ(string("test"), token.getNormalizedText());
    ASSERT_EQ((uint32_t)0, token.getPosition());

    ASSERT_TRUE(tokenizer.next(token));
    ASSERT_EQ(string(" "), token.getNormalizedText());
    ASSERT_EQ((uint32_t)1, token.getPosition());

    ASSERT_TRUE(tokenizer.next(token));
    ASSERT_EQ(string("tokenizer"), token.getNormalizedText());
    ASSERT_EQ((uint32_t)2, token.getPosition());

    ASSERT_TRUE(!tokenizer.next(token));
    ASSERT_TRUE(!tokenizer.next(token));
}

}} // namespace build_service::analyzer
