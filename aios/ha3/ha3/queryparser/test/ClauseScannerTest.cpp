#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/ClauseScanner.h>
#include <ha3/queryparser/ClauseScanner.h>
#include <ha3/queryparser/ClauseBisonParser.hh>
#include <string>
#include <sstream>

using namespace std;

BEGIN_HA3_NAMESPACE(queryparser);

class ClauseScannerTest : public TESTBASE {
public:
    ClauseScannerTest();
    ~ClauseScannerTest();
public:
    void setUp();
    void tearDown();
protected:
    ClauseScanner::semantic_type _yylval;
    ClauseScanner::location_type _yylocation;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(queryparser, ClauseScannerTest);


ClauseScannerTest::ClauseScannerTest() { 
}

ClauseScannerTest::~ClauseScannerTest() { 
}

void ClauseScannerTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void ClauseScannerTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(ClauseScannerTest, testScannerIntegerNumber) {
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "123";
    istringstream iss(queryText);
    ClauseScanner clauseScanner(&iss, &cout);
    ClauseScanner::token_type tokenType;

    tokenType = clauseScanner.lex(&_yylval, &_yylocation);
    unique_ptr<string> yylval_ptr(_yylval.stringVal);
    ASSERT_EQ(ClauseScanner::token::INTEGER, tokenType);
    ASSERT_EQ(string("123"), *(_yylval.stringVal));

    tokenType = clauseScanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(ClauseScanner::token::END, tokenType);
}

TEST_F(ClauseScannerTest, testScannerHexIntegerNumber) {
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "0x123";
    istringstream iss(queryText);
    ClauseScanner clauseScanner(&iss, &cout);
    ClauseScanner::token_type tokenType;

    tokenType = clauseScanner.lex(&_yylval, &_yylocation);
    unique_ptr<string> yylval_ptr(_yylval.stringVal);
    ASSERT_EQ(ClauseScanner::token::INTEGER, tokenType);
    ASSERT_EQ(string("0x123"), *(_yylval.stringVal));

    tokenType = clauseScanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(ClauseScanner::token::END, tokenType);
}

TEST_F(ClauseScannerTest, testScannerFloatNumber) {
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "123.456";
    istringstream iss(queryText);
    ClauseScanner clauseScanner(&iss, &cout);
    ClauseScanner::token_type tokenType;

    tokenType = clauseScanner.lex(&_yylval, &_yylocation);
    unique_ptr<string> yylval_ptr(_yylval.stringVal);
    ASSERT_EQ(ClauseScanner::token::FLOAT, tokenType);
    ASSERT_EQ(string("123.456"), *(_yylval.stringVal));

    tokenType = clauseScanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(ClauseScanner::token::END, tokenType);
}

TEST_F(ClauseScannerTest, testScannerPhraseEn) {
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "\"abc\"";
    istringstream iss(queryText);
    ClauseScanner clauseScanner(&iss, &cout);
    ClauseScanner::token_type tokenType;

    tokenType = clauseScanner.lex(&_yylval, &_yylocation);
    unique_ptr<string> yylval_ptr(_yylval.stringVal);
    ASSERT_EQ(ClauseScanner::token::PHRASE_STRING, tokenType);
    ASSERT_EQ(string("abc"), *(_yylval.stringVal));

    tokenType = clauseScanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(ClauseScanner::token::END, tokenType);
}

TEST_F(ClauseScannerTest, testScannerPhraseTwice) {
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "\"abc\\\"\"\"def\"";
    istringstream iss(queryText);
    ClauseScanner clauseScanner(&iss, &cout);
    ClauseScanner::token_type tokenType;

    tokenType = clauseScanner.lex(&_yylval, &_yylocation);
    unique_ptr<string> yylval_ptr(_yylval.stringVal);
    ASSERT_EQ(ClauseScanner::token::PHRASE_STRING, tokenType);
    ASSERT_EQ(string("abc\""), *(_yylval.stringVal));

    tokenType = clauseScanner.lex(&_yylval, &_yylocation);
    unique_ptr<string> yylval_ptr1(_yylval.stringVal);
    ASSERT_EQ(ClauseScanner::token::PHRASE_STRING, tokenType);
    ASSERT_EQ(string("def"), *(_yylval.stringVal));

    tokenType = clauseScanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(ClauseScanner::token::END, tokenType);
}

TEST_F(ClauseScannerTest, testScannerPhraseCn) {
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "\"\u4e2d\u6587\"";
    istringstream iss(queryText);
    ClauseScanner clauseScanner(&iss, &cout);
    ClauseScanner::token_type tokenType;

    tokenType = clauseScanner.lex(&_yylval, &_yylocation);
    unique_ptr<string> yylval_ptr(_yylval.stringVal);
    ASSERT_EQ(ClauseScanner::token::PHRASE_STRING, tokenType);
    ASSERT_EQ(string("\u4e2d\u6587"), *(_yylval.stringVal));

    tokenType = clauseScanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(ClauseScanner::token::END, tokenType);
}

TEST_F(ClauseScannerTest, testScannerKeyWord) {
    HA3_LOG(DEBUG, "Begin Test!");
    string queryText = "group_key";
    istringstream iss(queryText);
    ClauseScanner clauseScanner(&iss, &cout);
    ClauseScanner::token_type tokenType;

    tokenType = clauseScanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(ClauseScanner::token::GROUP_KEY, tokenType);

    tokenType = clauseScanner.lex(&_yylval, &_yylocation);
    ASSERT_EQ(ClauseScanner::token::END, tokenType);
}

END_HA3_NAMESPACE(queryparser);

