#include "build_service/test/unittest.h"
#include "build_service/analyzer/SuffixPrefixTokenizer.h"
#include "build_service/util/FileUtil.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace testing;
using namespace build_service::util;

namespace build_service {
namespace analyzer {

class SuffixPrefixTokenizerTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();

protected:
    void testCase(const string &str, uint32_t maxTokenNum,
                  uint32_t preMaxTokenNum,
                  const string &expectedTokensStr);
};

void SuffixPrefixTokenizerTest::setUp() {
}

void SuffixPrefixTokenizerTest::tearDown() {
}

TEST_F(SuffixPrefixTokenizerTest, testInit) {
    KeyValueMap kvMap;

    SuffixPrefixTokenizer tokenizer;
    ASSERT_EQ(SuffixPrefixTokenizer::DEFAULT_SUFFIX_LEN, tokenizer._maxSuffixLen);

    ASSERT_TRUE(tokenizer.init(kvMap, config::ResourceReaderPtr()));
    ASSERT_EQ(SuffixPrefixTokenizer::DEFAULT_SUFFIX_LEN, tokenizer._maxSuffixLen);

    kvMap["max_suffix_len"] = "12";
    ASSERT_TRUE(tokenizer.init(kvMap, config::ResourceReaderPtr()));
    ASSERT_EQ(12u, tokenizer._maxSuffixLen);

    kvMap["max_suffix_len"] = "1a2";
    ASSERT_FALSE(tokenizer.init(kvMap, config::ResourceReaderPtr()));
}

TEST_F(SuffixPrefixTokenizerTest, testTokenize) {
    testCase("", 8, 3, ""); // empty string
    testCase("a", 8, 3, "a"); // ascii
    testCase("abcdefghi", 3, 3, "i,h,hi,g,gh,ghi");
    testCase("我爱编程-你笑了", 5, 3, "了,笑,笑了,你,你笑,你笑了,-,-你,-你笑,程,程-,程-你");

    string str = "abc" + string(1, MULTI_VALUE_SEPARATOR) + "bcd";
    testCase(str, 2, 2, "c,b,bc,d,c,cd"); // multi value

    str = "DII框架" + string(1, MULTI_VALUE_SEPARATOR) + "BS框架";
    testCase(str, 2, 2, "架,框,框架,架,框,框架"); // multi value
}


void SuffixPrefixTokenizerTest::testCase(
        const string &str, uint32_t maxTokenNum,
        uint32_t preMaxTokenNum,
        const string &expectedTokensStr)
{
    vector<string> expectedTokens = StringUtil::split(expectedTokensStr, ",");

    SuffixPrefixTokenizer tokenizer(maxTokenNum, preMaxTokenNum);
    tokenizer.tokenize(str.c_str(), str.length());

    uint32_t index = 0;
    Token token;
    while (tokenizer.next(token)) {
        ASSERT_TRUE(token.isBasicRetrieve());
        ASSERT_FALSE(token.isDelimiter());
        ASSERT_EQ(expectedTokens[index], token.getNormalizedText());
        index++;
    }
    ASSERT_EQ(index, expectedTokens.size());

    // tokenize twice
    tokenizer.tokenize(str.c_str(), str.length());
    index = 0;
    while (tokenizer.next(token)) {
        ASSERT_TRUE(token.isBasicRetrieve());
        ASSERT_FALSE(token.isDelimiter());
        ASSERT_EQ(expectedTokens[index], token.getNormalizedText());
        index++;
    }
    ASSERT_EQ(index, expectedTokens.size());

    // clone
    Tokenizer *clonedOne = tokenizer.clone();
    clonedOne->tokenize(str.c_str(), str.length());
    index = 0;
    while (clonedOne->next(token)) {
        ASSERT_TRUE(token.isBasicRetrieve());
        ASSERT_FALSE(token.isDelimiter());
        ASSERT_EQ(expectedTokens[index], token.getNormalizedText());
        index++;
    }
    ASSERT_EQ(index, expectedTokens.size());
    delete clonedOne;
}

}
}
