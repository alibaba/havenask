#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <sstream>
#include <ha3/queryparser/Scanner.h>
#include <sstream>
#include <ha3/queryparser/Scanner.h>
#include <autil/TimeUtility.h>
#include <iostream>

using namespace std;
USE_HA3_NAMESPACE(util);
BEGIN_HA3_NAMESPACE(queryparser);

class ScannerUnitPerfTest : public TESTBASE {
public:
    ScannerUnitPerfTest();
    ~ScannerUnitPerfTest();
public:
    void setUp();
    void tearDown();
protected:
    void parseOneQuery(std::istringstream &iss);
    Scanner::semantic_type _yylval;
    Scanner::location_type _yylocation;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(queryparser, ScannerUnitPerfTest);


ScannerUnitPerfTest::ScannerUnitPerfTest() { 
}

ScannerUnitPerfTest::~ScannerUnitPerfTest() { 
}

void ScannerUnitPerfTest::setUp() { 
}

void ScannerUnitPerfTest::tearDown() {

}


void ScannerUnitPerfTest::parseOneQuery(istringstream &iss) {
    iss.clear();
    iss.seekg(ios_base::beg);
    Scanner scanner(&iss, &cout);
    Scanner::token_type tokenType;
    while((tokenType = scanner.lex(&_yylval, &_yylocation)) != 0) {
        if (tokenType == Scanner::token::IDENTIFIER
            || tokenType == Scanner::token::WORDS_STRING
            || tokenType == Scanner::token::PHRASE_STRING 
	    || tokenType == Scanner::token::CJK_STRING) 
        {
            delete _yylval.stringVal;
        }
    }
}

TEST_F(ScannerUnitPerfTest, testScannerPerf) {
    HA3_LOG(DEBUG, "Begin Test!");
    istringstream iss(string("default.phrase:'aaa bbb \\'ccc ddd eee' AND"
                             " default.fieldname:\u674e\u91d1\u8f89"
                             " OR \u90ed\u745e\u6770"));
    HA3_LOG(DEBUG, "iss_string:%s", iss.str().c_str());
    const int32_t CYCLE_COUNT = 10000;
    const int32_t TIME_COST_PER_QUERY = 30;
    const int32_t DELTA_COST_PER_QUERY = 10;
//    const char *filename = "~/svn/prof.log";
//    ProfilerStart(filename);
    uint64_t startTime = TimeUtility::currentTime();
    for (int i = 0; i < CYCLE_COUNT; i++) {
        parseOneQuery(iss);
    }
//    ProfilerStop();
    uint64_t endTime = TimeUtility::currentTime();
    ASSERT_TRUE_DOUBLES_EQUAL(CYCLE_COUNT * TIME_COST_PER_QUERY, 
                                 endTime - startTime,
                                 CYCLE_COUNT * DELTA_COST_PER_QUERY);
    HA3_LOG(DEBUG, "Scanner performance time cost: (%lu us)", endTime - startTime);
}

END_HA3_NAMESPACE(queryparser);
