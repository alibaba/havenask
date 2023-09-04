#include "build_service/analyzer/PrefixTokenizer.h"

#include "autil/StringUtil.h"
#include "build_service/test/unittest.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace autil;
using namespace testing;
using namespace build_service::util;

namespace build_service { namespace analyzer {

class PrefixTokenizerTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

protected:
    void innerTestTokenize(const string& str, uint32_t maxTokenNum, bool includeSelf, const string& expectedTokensStr);
};

void PrefixTokenizerTest::setUp() {}

void PrefixTokenizerTest::tearDown() {}

TEST_F(PrefixTokenizerTest, testInit)
{
    KeyValueMap kvMap;

    PrefixTokenizer tokenizer;
    ASSERT_EQ(PrefixTokenizer::DEFAULT_MAX_TOKEN_NUM, tokenizer._maxTokenNum);
    ASSERT_TRUE(!tokenizer._includeSelf);

    ASSERT_TRUE(tokenizer.init(kvMap, config::ResourceReaderPtr()));
    ASSERT_EQ(PrefixTokenizer::DEFAULT_MAX_TOKEN_NUM, tokenizer._maxTokenNum);
    ASSERT_TRUE(!tokenizer._includeSelf);

    kvMap["max_token_num"] = "12";
    kvMap["include_self"] = "true";
    ASSERT_TRUE(tokenizer.init(kvMap, config::ResourceReaderPtr()));
    ASSERT_EQ(12u, tokenizer._maxTokenNum);
    ASSERT_TRUE(tokenizer._includeSelf);

    kvMap["max_token_num"] = "1a2";
    ASSERT_FALSE(tokenizer.init(kvMap, config::ResourceReaderPtr()));

    kvMap["max_token_num"] = "12";
    kvMap["include_self"] = "abc";
    ASSERT_FALSE(tokenizer.init(kvMap, config::ResourceReaderPtr()));
}

TEST_F(PrefixTokenizerTest, testTokenize)
{
    innerTestTokenize("", 8, true, "");   // empty string
    innerTestTokenize("a", 8, true, "a"); // ascii
    innerTestTokenize("a", 8, false, "");
    innerTestTokenize("abcdefghi", 8, true, "a,ab,abc,abcd,abcde,abcdef,abcdefg,abcdefgh");
    innerTestTokenize("abcdefgh", 8, false, "a,ab,abc,abcd,abcde,abcdef,abcdefg");

    innerTestTokenize("我", 8, true, "我");
    innerTestTokenize("我", 8, false, "");
    innerTestTokenize("我我我我我我我我", 8, true,
                      "我,我我,我我我,我我我我,我我我我我,我我我我我我,我我我我我我我,我我我我我我我我");
    innerTestTokenize(" 我我", 2, true, " , 我");

    string str = "abc" + string(1, MULTI_VALUE_SEPARATOR) + "bcd";
    innerTestTokenize(str, 3, true, "a,ab,abc,b,bc,bcd"); // multi value
}

TEST_F(PrefixTokenizerTest, testTokenizeNonUtf8)
{
    PrefixTokenizer tokenizer;
    string rawDocFile = GET_TEST_DATA_PATH() + "/rawdoc.gb18030";
    string content;
    ASSERT_TRUE(fslib::util::FileUtil::readFile(rawDocFile, content));

    tokenizer.tokenize(content.c_str(), content.length());
    Token token;
    ASSERT_FALSE(tokenizer.next(token));
}

void PrefixTokenizerTest::innerTestTokenize(const string& str, uint32_t maxTokenNum, bool includeSelf,
                                            const string& expectedTokensStr)
{
    vector<string> expectedTokens = StringUtil::split(expectedTokensStr, ",");

    PrefixTokenizer tokenizer(maxTokenNum, includeSelf);
    tokenizer.tokenize(str.c_str(), str.length());

    uint32_t pos = 0;
    Token token;
    while (tokenizer.next(token)) {
        ASSERT_TRUE(pos < expectedTokens.size()) << "str: " << str << ", includeSelf: " << includeSelf;
        ASSERT_EQ(pos, token.getPosition());
        ASSERT_TRUE(token.isBasicRetrieve());
        ASSERT_FALSE(token.isDelimiter());
        ASSERT_EQ(expectedTokens[pos], token.getNormalizedText());
        pos++;
    }
    ASSERT_EQ(pos, expectedTokens.size());

    // tokenize twice
    tokenizer.tokenize(str.c_str(), str.length());
    pos = 0;
    while (tokenizer.next(token)) {
        ASSERT_TRUE(pos < expectedTokens.size()) << "str: " << str << ", includeSelf: " << includeSelf;
        ASSERT_EQ(pos, token.getPosition());
        ASSERT_TRUE(token.isBasicRetrieve());
        ASSERT_FALSE(token.isDelimiter());
        ASSERT_EQ(expectedTokens[pos], token.getNormalizedText());
        pos++;
    }
    ASSERT_EQ(pos, expectedTokens.size());

    // clone
    indexlibv2::analyzer::ITokenizer* clonedOne = tokenizer.clone();
    clonedOne->tokenize(str.c_str(), str.length());
    pos = 0;
    while (clonedOne->next(token)) {
        ASSERT_TRUE(pos < expectedTokens.size()) << "str: " << str << ", includeSelf: " << includeSelf;
        ASSERT_EQ(pos, token.getPosition());
        ASSERT_TRUE(token.isBasicRetrieve());
        ASSERT_FALSE(token.isDelimiter());
        ASSERT_EQ(expectedTokens[pos], token.getNormalizedText());
        pos++;
    }
    ASSERT_EQ(pos, expectedTokens.size());

    delete clonedOne;
}

}} // namespace build_service::analyzer
