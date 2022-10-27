#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/DistinctParser.h>

using namespace std;
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(queryparser);

class DistinctParserTest : public TESTBASE {
public:
    DistinctParserTest();
    ~DistinctParserTest();
public:
    void setUp();
    void tearDown();
protected:
    common::ErrorResult *_errorResult;
    DistinctParser *_parser;
    common::DistinctDescription *_distDescription;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(queryparser, DistinctParserTest);


DistinctParserTest::DistinctParserTest() { 
    _errorResult = NULL;
    _distDescription = NULL;
    _parser = NULL;
}

DistinctParserTest::~DistinctParserTest() { 
}

void DistinctParserTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _errorResult = new ErrorResult();
    _parser = new DistinctParser(_errorResult);
    _distDescription = new DistinctDescription;
}

void DistinctParserTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    delete _parser;
    _parser = NULL;
    delete _distDescription;
    _distDescription = NULL;
    delete _errorResult;
}

TEST_F(DistinctParserTest, testSetDistinctCount){
    string value = "0";
    _parser->setDistinctCount(_distDescription, value);
    ASSERT_TRUE(!_errorResult->hasError());
    ASSERT_EQ(0, _distDescription->getDistinctCount());

    value = "9999999999";
    _parser->setDistinctCount(_distDescription, value);
    ASSERT_TRUE(_errorResult->hasError());
    ASSERT_EQ(ERROR_DISTINCT_COUNT, _errorResult->getErrorCode());
    ASSERT_EQ(0, _distDescription->getDistinctCount());    
}

TEST_F(DistinctParserTest, testSetDistinctTimes){

    string value = "0";
    _parser->setDistinctTimes(_distDescription, value);
    ASSERT_TRUE(!_errorResult->hasError());
    ASSERT_EQ(0, _distDescription->getDistinctTimes());

    value = "9999999999";
    _parser->setDistinctTimes(_distDescription, value);
    ASSERT_TRUE(_errorResult->hasError());
    ASSERT_EQ(ERROR_DISTINCT_TIMES, _errorResult->getErrorCode());
    ASSERT_EQ(0, _distDescription->getDistinctTimes());
}
TEST_F(DistinctParserTest, testSetReservedFlag){
    string value = "false";
    _parser->setReservedFlag(_distDescription, value);
    ASSERT_TRUE(!_errorResult->hasError());
    ASSERT_TRUE(!_distDescription->getReservedFlag());

    value = "true";
    _parser->setReservedFlag(_distDescription, value);
    ASSERT_TRUE(!_errorResult->hasError());
    ASSERT_TRUE(_distDescription->getReservedFlag());

    value = "fal";
    _parser->setReservedFlag(_distDescription, value);
    ASSERT_TRUE(_errorResult->hasError());
    ASSERT_TRUE(_distDescription->getReservedFlag());
    ASSERT_EQ(ERROR_DISTINCT_RESERVED_FLAG, _errorResult->getErrorCode());

}
TEST_F(DistinctParserTest, testSetGradeThresholds){
    
    vector<string *> *strVec = new vector<string *>;
    string *val1 = new string("567");
    string *val2 = new string("-1");
    string *val3 = new string("568");
    strVec->push_back(val1);
    strVec->push_back(val2);
    strVec->push_back(val3);
    _parser->setGradeThresholds(_distDescription, strVec);
    ASSERT_TRUE(!_errorResult->hasError());
    const vector<double> &valueVec = _distDescription->getGradeThresholds();
    ASSERT_EQ(size_t(3), valueVec.size());
    ASSERT_DOUBLE_EQ(-1, valueVec[0]);
    ASSERT_DOUBLE_EQ(567, valueVec[1]);
    ASSERT_DOUBLE_EQ(568, valueVec[2]);


    vector<string *> *strVec1 = new vector<string *>;
    string *val4 = new string("567");
    string *val5 = new string;
    val5->append(400, '9');
    string *val6 = new string("568");
    strVec1->push_back(val4);
    strVec1->push_back(val5);
    strVec1->push_back(val6);
    _parser->setGradeThresholds(_distDescription, strVec1);
    ASSERT_TRUE(_errorResult->hasError());
    
    const vector<double> &valueVec1 = _distDescription->getGradeThresholds();
    ASSERT_EQ(size_t(3), valueVec1.size());
    ASSERT_DOUBLE_EQ(-1, valueVec1[0]);
    ASSERT_DOUBLE_EQ(567, valueVec1[1]);
    ASSERT_DOUBLE_EQ(568, valueVec1[2]);
}

END_HA3_NAMESPACE(queryparser);

