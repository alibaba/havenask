#include "indexlib/analyzer/SimpleTokenizer.h"

#include <string>

#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::document;

namespace indexlibv2 { namespace analyzer {

class SimpleTokenizerTest : public TESTBASE
{
public:
    void setUp() override;
    void tearDown() override;

protected:
    void assertToken(const std::string& tokenStr, int32_t position, ITokenizer* tokenizerPtr, bool isDelimiter = false);

protected:
    SimpleTokenizer* _tokenizer;
    AnalyzerToken _token;
};

void SimpleTokenizerTest::assertToken(const string& tokenStr, int32_t position, ITokenizer* tokenizerPtr,
                                      bool isDelimiter)
{
    bool ret = tokenizerPtr->next(_token);
    ASSERT_TRUE(ret);
    ASSERT_EQ(tokenStr, _token.getNormalizedText());
    ASSERT_EQ((uint32_t)position, _token.getPosition());
    ASSERT_TRUE(isDelimiter == _token.isDelimiter());
    if (isDelimiter) {
        ASSERT_TRUE(!_token.isRetrieve());
    }
}

void SimpleTokenizerTest::setUp() { _tokenizer = new SimpleTokenizer(string(" ")); }

void SimpleTokenizerTest::tearDown() { DELETE_AND_SET_NULL(_tokenizer); }

TEST_F(SimpleTokenizerTest, testTokenizeEmptyString)
{
    string str("");
    _tokenizer->tokenize(str.data(), str.size());
    ASSERT_TRUE(!_tokenizer->next(_token));
}

TEST_F(SimpleTokenizerTest, testTokenizeChineseString)
{
    string str("菊花茶　abc");
    _tokenizer->tokenize(str.data(), str.size());
    assertToken("菊花茶　abc", 0, _tokenizer);
    ASSERT_TRUE(!_tokenizer->next(_token));

    SimpleTokenizer tokenizer;
    str = "园区太大容易迷路，\"导航在手\"\\\\很重要哦!";
    tokenizer.tokenize(str.data(), str.size());
    assertToken("园区太大容易迷路，\"导航在手\"\\\\很重要哦!", 0, &tokenizer);
    ASSERT_TRUE(!tokenizer.next(_token));

    str = "\345\233\255\345\214\272\345\244\252\345\244\247\345\256\271\346\230\223\350\277\267\350\267\257\357\274\214"
          "\"\345\257\274\350\210\252\345\234\250\346\211\213\"\\\345\276\210\351\207\215\350\246\201\345\223\246!";
    tokenizer.tokenize(str.data(), str.size());
    assertToken(
        "\345\233\255\345\214\272\345\244\252\345\244\247\345\256\271\346\230\223\350\277\267\350\267\257\357\274\214\""
        "\345\257\274\350\210\252\345\234\250\346\211\213\"\\\345\276\210\351\207\215\350\246\201\345\223\246!",
        0, &tokenizer);
    ASSERT_TRUE(!tokenizer.next(_token));
}

TEST_F(SimpleTokenizerTest, testTokenizeEnglishString)
{
    string str("Hello World");
    _tokenizer->tokenize(str.data(), str.size());
    assertToken("Hello", 0, _tokenizer);
    assertToken(" ", 1, _tokenizer, true);
    assertToken("World", 2, _tokenizer);
    ASSERT_TRUE(!_tokenizer->next(_token));
}

TEST_F(SimpleTokenizerTest, testTokenizeForTwoTime)
{
    string str("Hello World");
    _tokenizer->tokenize(str.data(), str.size());
    assertToken("Hello", 0, _tokenizer);
    assertToken(" ", 1, _tokenizer, true);
    assertToken("World", 2, _tokenizer);
    ASSERT_TRUE(!_tokenizer->next(_token));
    str = "Hello  World";
    _tokenizer->tokenize(str.data(), str.size());
    assertToken("Hello", 0, _tokenizer);
    assertToken(" ", 1, _tokenizer, true);
    assertToken(" ", 2, _tokenizer, true);
    assertToken("World", 3, _tokenizer);
    ASSERT_TRUE(!_tokenizer->next(_token));
}

TEST_F(SimpleTokenizerTest, testClone)
{
    string str("Hello World, Good Night");
    _tokenizer->tokenize(str.data(), str.size());
    assertToken("Hello", 0, _tokenizer);
    assertToken(" ", 1, _tokenizer, true);
    ITokenizer* newTokenizer = _tokenizer->clone();
    assertToken("World,", 2, _tokenizer);
    assertToken(" ", 3, _tokenizer, true);
    ASSERT_TRUE(!newTokenizer->next(_token));
    string str2("Alibaba, how are you");
    newTokenizer->tokenize(str2.data(), str2.size());
    assertToken("Alibaba,", 0, newTokenizer);
    assertToken("Good", 4, _tokenizer);
    assertToken(" ", 1, newTokenizer, true);
    assertToken("how", 2, newTokenizer);
    assertToken(" ", 3, newTokenizer, true);
    assertToken(" ", 5, _tokenizer, true);
    assertToken("Night", 6, _tokenizer);
    assertToken("are", 4, newTokenizer);
    assertToken(" ", 5, newTokenizer, true);
    assertToken("you", 6, newTokenizer);
    ASSERT_TRUE(!_tokenizer->next(_token));
    ASSERT_TRUE(!newTokenizer->next(_token));

    DELETE_AND_SET_NULL(newTokenizer);
}

}} // namespace indexlibv2::analyzer
