#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/Scanner.h>
#include <sstream>
#include <ha3/queryparser/Scanner.h>
#include <string>
#include <sstream>
#include <ha3/queryparser/BisonParser.hh>
#include <autil/TimeUtility.h>
//#include <google/profiler.h>

using namespace std;
BEGIN_HA3_NAMESPACE(queryparser);

class ScannerTest : public TESTBASE {
public:
    ScannerTest();
    ~ScannerTest();
public:
    void setUp();
    void tearDown();
protected:
    Scanner::semantic_type _yylval;
    Scanner::location_type _yylocation;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(queryparser, ScannerTest);


ScannerTest::ScannerTest() { 
}

ScannerTest::~ScannerTest() { 
}

void ScannerTest::setUp() { 
}

void ScannerTest::tearDown() { 
}

TEST_F(ScannerTest, testSimpleWord) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "abc";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    unique_ptr<string> yylval_ptr(_yylval.stringVal);
    ASSERT_EQ(Scanner::token::IDENTIFIER, tokenType);
    ASSERT_EQ(queryText, *(_yylval.stringVal));

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testSimpleWordSingleChar) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "a";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    std::unique_ptr<string> yylval_ptr(_yylval.stringVal);
    ASSERT_EQ(Scanner::token::IDENTIFIER, tokenType);
    ASSERT_EQ(queryText, *(_yylval.stringVal));

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testSimpleWordWithUnderScore) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "_abc";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    std::unique_ptr<string> yylval_ptr(_yylval.stringVal);
    ASSERT_EQ(Scanner::token::IDENTIFIER, tokenType);
    ASSERT_EQ(queryText, *(_yylval.stringVal));

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testWordsWithChineseCharacters) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "_abc \u4e2d\u6587word";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    std::unique_ptr<string> yylval_ptr(_yylval.stringVal);
    ASSERT_EQ(Scanner::token::IDENTIFIER, tokenType);
    HA3_LOG(DEBUG, "token 1:|%s|", _yylval.stringVal->c_str());
    ASSERT_EQ(string("_abc"), *(_yylval.stringVal));

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::CJK_STRING, tokenType);
    std::unique_ptr<string> yylval_ptr2(_yylval.stringVal);
    HA3_LOG(DEBUG, "token 2:|%s|", _yylval.stringVal->c_str());
    ASSERT_EQ(string("\u4e2d\u6587word"), *(_yylval.stringVal));

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testNumberWord) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "123456789";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    std::unique_ptr<string> yylval_ptr(_yylval.stringVal);
    ASSERT_EQ(Scanner::token::NUMBER, tokenType);
    ASSERT_EQ(string("123456789"), *_yylval.stringVal);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testSignedNumberWord) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "+123456";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    std::unique_ptr<string> yylval_ptr(_yylval.stringVal);
    ASSERT_EQ(Scanner::token::NUMBER, tokenType);
    ASSERT_EQ(string("+123456"), *_yylval.stringVal);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testInvalidSignedNumberWord) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "++123456";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::CJK_STRING, tokenType);
    ASSERT_EQ(string("++123456"), *(_yylval.stringVal));
    std::unique_ptr<string> yylval_ptr2(_yylval.stringVal);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testNegativeNumberWord) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "-123456";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    std::unique_ptr<string> yylval_ptr2(_yylval.stringVal);
    ASSERT_EQ(Scanner::token::NUMBER, tokenType);
    ASSERT_EQ(string("-123456"), *_yylval.stringVal);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testQuotedNumberWord) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "'123456789'777";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    {
        tokenType = scanner.lex(&_yylval, &_yylocation);
        std::unique_ptr<string> yylval_ptr(_yylval.stringVal);
        ASSERT_EQ(Scanner::token::CJK_STRING, tokenType);
        ASSERT_EQ(string("123456789"), *(_yylval.stringVal));
    }
    {
        tokenType = scanner.lex(&_yylval, &_yylocation);
        std::unique_ptr<string> yylval_ptr(_yylval.stringVal);
        ASSERT_EQ(Scanner::token::NUMBER, tokenType);
        ASSERT_EQ(string("777"), *_yylval.stringVal);
    }
    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testPhraseWord) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "\"aaa bbb\"";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::PHRASE_STRING, tokenType);
    ASSERT_EQ(string("aaa bbb"), *(_yylval.stringVal));
    std::unique_ptr<string> yylval_ptr2(_yylval.stringVal);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testPhraseWithAndOpString) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "\"aaa AND bbb\"";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::PHRASE_STRING, tokenType);
    ASSERT_EQ(string("aaa AND bbb"), *(_yylval.stringVal));
    std::unique_ptr<string> yylval_ptr2(_yylval.stringVal);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testPhraseWithOrOpString) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "\"aaa OR bbb\"";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::PHRASE_STRING, tokenType);
    ASSERT_EQ(string("aaa OR bbb"), *(_yylval.stringVal));
    std::unique_ptr<string> yylval_ptr2(_yylval.stringVal);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testAndOP) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "xxx AND yyy";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::IDENTIFIER, tokenType);
    ASSERT_EQ(string("xxx"), *(_yylval.stringVal));
    std::unique_ptr<string> yylval_ptr1(_yylval.stringVal);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::AND, tokenType);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::IDENTIFIER, tokenType);
    ASSERT_EQ(string("yyy"), *(_yylval.stringVal));
    std::unique_ptr<string> yylval_ptr2(_yylval.stringVal);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testAndNotOP) {
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "xxx ANDNOT yyy";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::IDENTIFIER, tokenType);
    ASSERT_EQ(string("xxx"), *(_yylval.stringVal));
    std::unique_ptr<string> yylval_ptr1(_yylval.stringVal);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::ANDNOT, tokenType);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::IDENTIFIER, tokenType);
    ASSERT_EQ(string("yyy"), *(_yylval.stringVal));
    std::unique_ptr<string> yylval_ptr2(_yylval.stringVal);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}


TEST_F(ScannerTest, testRankOP) {
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "xxx RANK yyy";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::IDENTIFIER, tokenType);
    ASSERT_EQ(string("xxx"), *(_yylval.stringVal));
    std::unique_ptr<string> yylval_ptr1(_yylval.stringVal);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::RANK, tokenType);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::IDENTIFIER, tokenType);
    ASSERT_EQ(string("yyy"), *(_yylval.stringVal));
    std::unique_ptr<string> yylval_ptr2(_yylval.stringVal);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testOrOP) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "\u674e\u91d1\u8f89 OR \u90ed\u745e\u6770";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::CJK_STRING, tokenType);
    ASSERT_EQ(string("\u674e\u91d1\u8f89"), *(_yylval.stringVal));
    std::unique_ptr<string> yylval_ptr1(_yylval.stringVal);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::OR, tokenType);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::CJK_STRING, tokenType);
    ASSERT_EQ(string("\u90ed\u745e\u6770"), *(_yylval.stringVal));
    std::unique_ptr<string> yylval_ptr2(_yylval.stringVal);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testPhraseAndOP) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "\"abc cde\" AND fgh";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::PHRASE_STRING, tokenType);
    ASSERT_EQ(string("abc cde"), *(_yylval.stringVal));
    std::unique_ptr<string> yylval_ptr1(_yylval.stringVal);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::AND, tokenType);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::IDENTIFIER, tokenType);
    ASSERT_EQ(string("fgh"), *(_yylval.stringVal));
    std::unique_ptr<string> yylval_ptr2(_yylval.stringVal);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

// SPECIAL_SYMBOL   [:;\]\[()^,|&#]
TEST_F(ScannerTest, testSpecialChar) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = " :\")\"\t( \t^\t,\t|\t&\t#\t";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token_type(':'), tokenType);
    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::PHRASE_STRING, tokenType);
    ASSERT_EQ(string(")"), *(_yylval.stringVal));
    std::unique_ptr<string> yylval_ptr1(_yylval.stringVal);
    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token_type('('), tokenType);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token_type('^'), tokenType);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token_type(','), tokenType);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token_type('|'), tokenType);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token_type('&'), tokenType);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token_type('#'), tokenType);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testWeakAnd) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "|| a | b || c > < ||| '||'";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token_type(Scanner::token::WEAKAND), tokenType);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token_type(Scanner::token::IDENTIFIER), tokenType);
    ASSERT_EQ(string("a"), *(_yylval.stringVal));
    std::unique_ptr<string> yylval_ptr1(_yylval.stringVal);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token_type('|'), tokenType);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token_type(Scanner::token::IDENTIFIER), tokenType);
    ASSERT_EQ(string("b"), *(_yylval.stringVal));
    std::unique_ptr<string> yylval_ptr2(_yylval.stringVal);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token_type(Scanner::token::WEAKAND), tokenType);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token_type(Scanner::token::IDENTIFIER), tokenType);
    ASSERT_EQ(string("c"), *(_yylval.stringVal));
    std::unique_ptr<string> yylval_ptr3(_yylval.stringVal);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token_type(Scanner::token::GREATER), tokenType);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token_type(Scanner::token::LESS), tokenType);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token_type(Scanner::token::WEAKAND), tokenType);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token_type('|'), tokenType);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::CJK_STRING, tokenType);
    ASSERT_EQ(string("||"), *(_yylval.stringVal));
    std::unique_ptr<string> yylval_ptr4(_yylval.stringVal);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}


TEST_F(ScannerTest, testCompareOp) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "=>=<=><";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::EQUAL, tokenType);
    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::GE, tokenType);
    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::LE, tokenType);
    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::GREATER, tokenType);
    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::LESS, tokenType);

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testControlChar) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "\x03\x01\x0D\x10\xEF";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testIllegalChineseChar) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "\u2E7F";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testIllegalChineseCharUpper) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "\ua000";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testSingleQuoteInWordsString) {
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "'\\''";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    std::unique_ptr<string> yylval_ptr(_yylval.stringVal);
    ASSERT_EQ(Scanner::token::CJK_STRING, tokenType);
    ASSERT_EQ(string("'"), *(_yylval.stringVal));

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testDoublequoteinwordsstring) {
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "'\"'";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    std::unique_ptr<string> yylval_ptr(_yylval.stringVal);
    ASSERT_EQ(Scanner::token::CJK_STRING, tokenType);
    ASSERT_EQ(string("\""), *(_yylval.stringVal));

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testQuoteInPhraseString) {
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "\"\\\"\\\\'\"";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    unique_ptr<string> yylval_ptr(_yylval.stringVal);
    ASSERT_EQ(Scanner::token::PHRASE_STRING, tokenType);
    ASSERT_EQ(string("\"\\'"), *(_yylval.stringVal));

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testQuoteEscapeString) {
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "'aaa \\y'";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    unique_ptr<string> yylval_ptr(_yylval.stringVal);
    ASSERT_EQ(Scanner::token::CJK_STRING, tokenType);
    ASSERT_EQ(string("aaa y"), *(_yylval.stringVal));

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testSpecialSymbolsInTerm) {
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "Android2.0 utf-8 c++ 1024*768";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    unique_ptr<string> yylval_ptr1(_yylval.stringVal);
    ASSERT_EQ(Scanner::token::CJK_STRING, tokenType);
    ASSERT_EQ(string("Android2.0"), *(_yylval.stringVal));

    tokenType = scanner.lex(&_yylval, &_yylocation);
    unique_ptr<string> yylval_ptr2(_yylval.stringVal);
    ASSERT_EQ(Scanner::token::CJK_STRING, tokenType);
    ASSERT_EQ(string("utf-8"), *(_yylval.stringVal));

    tokenType = scanner.lex(&_yylval, &_yylocation);
    unique_ptr<string> yylval_ptr3(_yylval.stringVal);
    ASSERT_EQ(Scanner::token::CJK_STRING, tokenType);
    ASSERT_EQ(string("c++"), *(_yylval.stringVal));

    tokenType = scanner.lex(&_yylval, &_yylocation);
    unique_ptr<string> yylval_ptr4(_yylval.stringVal);
    ASSERT_EQ(Scanner::token::CJK_STRING, tokenType);
    ASSERT_EQ(string("1024*768"), *(_yylval.stringVal));

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testIllegalUTF8Character) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "\xE2\x7F\x7F\xEA\xC0\xC0";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}

TEST_F(ScannerTest, testFullWidthCharacter) {
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "。 ， ！ ０ ９ Ａ Ｚ ａ ｚ ￥";
    istringstream iss(queryText);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;
    vector<string> result;
    result.push_back(string("。"));
    result.push_back(string("，"));
    result.push_back(string("！"));
    result.push_back(string("０"));
    result.push_back(string("９"));
    result.push_back(string("Ａ"));
    result.push_back(string("Ｚ"));
    result.push_back(string("ａ"));
    result.push_back(string("ｚ"));
    result.push_back(string("￥"));
    for(size_t i = 0; i < result.size(); ++i) {
        tokenType = scanner.lex(&_yylval, &_yylocation);
        unique_ptr<string> yylval_ptr(_yylval.stringVal);
        ASSERT_EQ(Scanner::token::CJK_STRING, tokenType);
        ASSERT_EQ(result[i], *(_yylval.stringVal));
    }

    tokenType = scanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(Scanner::token::END, tokenType);
}
 
END_HA3_NAMESPACE(queryparser);
