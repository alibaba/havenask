#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/Aggregator.h>
#include <ha3/search/AggregatorParser.h>
#include <ha3/index/AttributeReaderManager.h>
#include <suez/turing/expression/util/FieldInfos.h>
#include <vector>
#include <ha3/search/NormalAggregator.h>
#include <ha3/search/RangeAggregator.h>
#include <ha3/test/test.h>
#include <map>
#include <ha3/index/SegmentInfo.h>
#include <suez/turing/expression/util/AttributeInfos.h>

using namespace std;
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(search);

class AggregatorParserTest : public TESTBASE {
public:
    AggregatorParserTest();
    ~AggregatorParserTest();
public:
//     void setUp();
//     void tearDown();
protected:
    AggregatorParser *_aggParser;
    Aggregator *_agg;

    config::FieldInfo *_fieldInfo;
    index::SegmentInfos _segmentInfos;
    index::AttributeReaderManager *_manager;

    std::vector<config::FieldInfo*> _fieldInfoVector;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, AggregatorParserTest);


AggregatorParserTest::AggregatorParserTest() { 
    _aggParser = NULL;
    _agg = NULL;
}

AggregatorParserTest::~AggregatorParserTest() { 
}

// void AggregatorParserTest::setUp() { 
//     HA3_LOG(DEBUG, "setUp!");
//     string path = TEST_DATA_PATH;
//     path += "/newroot/simple/";

//     AttributeInfos attrInfos(path.c_str());
//     AttributeInfo *info = new AttributeInfo(attrInfos.getPath().c_str());
//     _fieldInfo = new FieldInfo();
//     _fieldInfoVector.push_back(_fieldInfo);
//     _fieldInfo->fieldType = ft_integer;
//     info->setAttrName("uid");
//     info->setFieldInfo(_fieldInfo);
//     attrInfos.addAttributeInfo(info);
    
//      info = new AttributeInfo(attrInfos.getPath().c_str());
//      _fieldInfo = new FieldInfo();
//      _fieldInfoVector.push_back(_fieldInfo);
//      _fieldInfo->fieldType = ft_integer;
//     info->setAttrName("price");
//     info->setFieldInfo(_fieldInfo);
//     attrInfos.addAttributeInfo(info);

//     _segmentInfos.clear();
//     SegmentInfoData segInfoData;
//     segInfoData.serial = 1;
//     segInfoData.maxCount = 1000;
//     segInfoData.docCount = 2;
//     segInfoData.disposeTime = util::NumericLimits<int>::max();
//     segInfoData.buildState = 'N';
//     segInfoData.buildMode = 'S';
//     strncpy(segInfoData.subDir, "index", MAX_SUBDIR_LENGTH);
//     _segmentInfos.push_back(SegmentInfo(segInfoData));

//     _manager = new AttributeReaderManager(_segmentInfos, attrInfos);
//     ASSERT_TRUE(_manager->init());

//     _aggParser = new AggregatorParser(_manager);
// }

// void AggregatorParserTest::tearDown() { 
//     HA3_LOG(DEBUG, "tearDown!");
//     delete _aggParser;
//     _aggParser = NULL;

//     if(_agg) {
//         delete _agg;
//         _agg = NULL;
//     }
//     if (_manager) {
//         delete _manager;
//         _manager = NULL;
//     }
//     for(vector<FieldInfo *>::iterator it = _fieldInfoVector.begin();
//         it != _fieldInfoVector.end(); it ++ ) 
//     {
//         delete (*it);
//     }
//     _fieldInfoVector.clear();
// }


// void AggregatorParserTest::testParseWithOneFunc() { 
//     HA3_LOG(DEBUG, "Begin Test!");
//     string aggStr = "uid();max(price)";
//     _agg = _aggParser->parse(aggStr);
    
//     ASSERT_TRUE(_agg);
//     NormalAggregator<int32_t> *normalAggInt 
//         = dynamic_cast<NormalAggregator<int32_t> *>(_agg);
//     ASSERT_TRUE(normalAggInt);

//     ASSERT_EQ((int32_t)1000, normalAggInt->getMaxKeyCount());

//     const NormalAggregator<int32_t>::FunctionVector& functions 
//         = normalAggInt->getAggregateFunctions();
//     ASSERT_EQ((size_t)1, functions.size());
    
//     AggregateFunction *function = functions[0];
//     ASSERT_EQ(string("max"), function->getFunctionType());
//     ASSERT_EQ(string("max(price)"), function->getFunctionName());
//     ASSERT_EQ(vt_int, function->getResultType());
//     HA3_LOG(DEBUG, "Finish Test!");
// }


// void AggregatorParserTest::testParseWithTwoFunc() { 
//     string aggStr = "uid(maxgroup:999);max(price),min(price)";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(_agg);
//     NormalAggregator<int32_t> *normalAggInt 
//         = dynamic_cast<NormalAggregator<int32_t> *>(_agg);
//     ASSERT_TRUE(normalAggInt);

//     ASSERT_EQ((int32_t)999, normalAggInt->getMaxKeyCount());

//     const NormalAggregator<int32_t>::FunctionVector& functions 
//         = normalAggInt->getAggregateFunctions();
//     ASSERT_EQ((size_t)2, functions.size());

//     AggregateFunction *function = functions[0];
//     ASSERT_TRUE(function);
//     ASSERT_EQ(string("max"), function->getFunctionType());
//     ASSERT_EQ(string("max(price)"), function->getFunctionName());
//     ASSERT_EQ(vt_int, function->getResultType());

//     function = functions[1];
//     ASSERT_TRUE(function);
//     ASSERT_EQ(string("min"), function->getFunctionType());
//     ASSERT_EQ(string("min(price)"), function->getFunctionName());
//     ASSERT_EQ(vt_int, function->getResultType());
// }

// void AggregatorParserTest::testParseWithFiveFunc() { 
//     string aggStr = "uid();max(price),min(price),count(),avg(price),sum(price)";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(_agg);
//     NormalAggregator<int32_t> *normalAggInt 
//         = dynamic_cast<NormalAggregator<int32_t> *>(_agg);
//     ASSERT_TRUE(normalAggInt);

//     ASSERT_EQ((int32_t)1000, normalAggInt->getMaxKeyCount());

//     const NormalAggregator<int32_t>::FunctionVector& functions = normalAggInt->getAggregateFunctions();
//     ASSERT_EQ((size_t)5, functions.size());

//     AggregateFunction *function = functions[0];
//     ASSERT_TRUE(function);
//     ASSERT_EQ(string("max"), function->getFunctionType());
//     ASSERT_EQ(string("max(price)"), function->getFunctionName());
//     ASSERT_EQ(vt_int, function->getResultType());

//     function = functions[1];
//     ASSERT_TRUE(function);
//     ASSERT_EQ(string("min"), function->getFunctionType());
//     ASSERT_EQ(string("min(price)"), function->getFunctionName());
//     ASSERT_EQ(vt_int, function->getResultType());

//     function = functions[2];
//     ASSERT_TRUE(function);
//     ASSERT_EQ(string("count"), function->getFunctionType());
//     ASSERT_EQ(string("count()"), function->getFunctionName());
//     ASSERT_EQ(vt_int, function->getResultType());

//     function = functions[3];
//     ASSERT_TRUE(function);
//     ASSERT_EQ(string("avg"), function->getFunctionType());
//     ASSERT_EQ(string("avg(price)"), function->getFunctionName());
//     ASSERT_EQ(vt_int, function->getResultType());

//     function = functions[4];
//     ASSERT_TRUE(function);
//     ASSERT_EQ(string("sum"), function->getFunctionType());
//     ASSERT_EQ(string("sum(price)"), function->getFunctionName());
//     ASSERT_EQ(vt_int, function->getResultType());
// }

// void AggregatorParserTest::testParseNoFunc() { 
//     string aggStr = "uid()";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseWithCountFunc() { 
//     string aggStr = "uid();count(*)";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(_agg);
//     NormalAggregator<int32_t> *normalAggInt 
//         = dynamic_cast<NormalAggregator<int32_t> *>(_agg);
//     ASSERT_TRUE(normalAggInt);

//     const NormalAggregator<int32_t>::FunctionVector& functions 
//         = normalAggInt->getAggregateFunctions();
//     ASSERT_EQ((size_t)1, functions.size());
//     AggregateFunction *function = functions[0];
//     ASSERT_EQ(string("count"), function->getFunctionType());
//     ASSERT_EQ(string("count(*)"), function->getFunctionName());
//     ASSERT_EQ(vt_int, function->getResultType());
// }

// void AggregatorParserTest::testParseWithCountFunc2() { 
//     string aggStr = "uid();count()";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(_agg);
//     NormalAggregator<int32_t> *normalAggInt 
//         = dynamic_cast<NormalAggregator<int32_t> *>(_agg);
//     ASSERT_TRUE(normalAggInt);

//     const NormalAggregator<int32_t>::FunctionVector& functions 
//         = normalAggInt->getAggregateFunctions();
//     ASSERT_EQ((size_t)1, functions.size());
//     AggregateFunction *function = functions[0];
//     ASSERT_EQ(string("count"), function->getFunctionType());
//     ASSERT_EQ(string("count()"), function->getFunctionName());
//     ASSERT_EQ(vt_int, function->getResultType());
// }

// void AggregatorParserTest::testParseWithInvalidAttribute() { 
//     string aggStr = "uid();max(price),min(xxx),count,avg(price),sum(price)";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseWithInvalidFunction() { 
//     string aggStr = "uid();maxxxx(price),min(price),count,avg(price),sum(price)";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseWithInvalidAggKey() { 
//     string aggStr = "uidxxx();max(price),min(price),count(),avg(price),sum(price)";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseWithInvalidExpr() { 
//     string aggStr = "uid(),max(price),min(price),count(),avg(price),sum(price)";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseWithWrongMaxGroup() { 
//     string aggStr = "uid(maxgroup:abc);min(price),count(),avg(price),sum(price)";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseMissMaxGroupValue() { 
//     string aggStr = "uid(maxgroup:);min(price),count(),avg(price),sum(price)";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseTwoRange() { 
//     string aggStr = "uid(range:10);min(price)";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(_agg);
//     RangeAggregator<int32_t> *rangeAggInt 
//         = dynamic_cast<RangeAggregator<int32_t> *>(_agg);
//     ASSERT_TRUE(rangeAggInt);

//     const std::vector<int32_t> vec = rangeAggInt->getRangeVector();
//     ASSERT_EQ((size_t)3, vec.size());
//     ASSERT_EQ((int32_t)10, vec[1]);
    
//     const NormalAggregator<int32_t>::FunctionVector& functions 
//         = rangeAggInt->getAggregateFunctions();
//     ASSERT_EQ((size_t)1, functions.size());
//     AggregateFunction *function = functions[0];
//     ASSERT_EQ(string("min"), function->getFunctionType());
//     ASSERT_EQ(string("min(price)"), function->getFunctionName());
//     ASSERT_EQ(vt_int, function->getResultType());
// }

// void AggregatorParserTest::testParseThreeRange() { 
//     string aggStr = "uid(range:10,100);min(price)";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(_agg);
//     RangeAggregator<int32_t> *rangeAggInt 
//         = dynamic_cast<RangeAggregator<int32_t> *>(_agg);
//     ASSERT_TRUE(rangeAggInt);

//     const std::vector<int32_t> vec = rangeAggInt->getRangeVector();
//     ASSERT_EQ((size_t)4, vec.size());
//     ASSERT_EQ((int32_t)util::NumericLimits<int32_t>::min(), vec[0]);
//     ASSERT_EQ((int32_t)10, vec[1]);
//     ASSERT_EQ((int32_t)100, vec[2]);

//     const NormalAggregator<int32_t>::FunctionVector& functions 
//         = rangeAggInt->getAggregateFunctions();
//     ASSERT_EQ((size_t)1, functions.size());
//     AggregateFunction *function = functions[0];
//     ASSERT_EQ(string("min"), function->getFunctionType());
//     ASSERT_EQ(string("min(price)"), function->getFunctionName());
//     ASSERT_EQ(vt_int, function->getResultType());
// }

// void AggregatorParserTest::testParseWrongRange() { 
//     HA3_LOG(DEBUG, "BeginTestXXXX!");
//     string aggStr = "uid(range:10,100,);min(price)";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseWrongRange2() { 
//     string aggStr = "uid(range:10,10);min(price)";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(_agg);
// }

// void AggregatorParserTest::testParseWrongRange3() { 
//     string aggStr = "uid(range:10,1);min(price)";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(_agg);
// }

// void AggregatorParserTest::testParseFloatThreeRange() { 
//     string aggStr = "price(range:0,10.0,100.0);count(),max(price)";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(_agg);
//     RangeAggregator<float> *rangeAggInt 
//         = dynamic_cast<RangeAggregator<float> *>(_agg);
//     ASSERT_TRUE(rangeAggInt);

//     const std::vector<float > vec = rangeAggInt->getRangeVector();
//     ASSERT_EQ((size_t)3, vec.size());
//     ASSERT_TRUE_DOUBLES_EQUAL((float )0, vec[0], 0.01);
//     ASSERT_TRUE_DOUBLES_EQUAL((float )10.0, vec[1], 0.01);
//     ASSERT_TRUE_DOUBLES_EQUAL((float )100.0, vec[2], 0.01);

//     const NormalAggregator<float >::FunctionVector& functions 
//         = rangeAggInt->getAggregateFunctions();
//     ASSERT_EQ((size_t)2, functions.size());
//     AggregateFunction *function = functions[0];
//     ASSERT_EQ(string(""), function->getFunctionName());
//     ASSERT_EQ(vt_long, function->getResultType());
//     function = functions[1];
//     ASSERT_EQ(string("price"), function->getFunctionName());
//     ASSERT_EQ(vt_float, function->getResultType());
// }

// void AggregatorParserTest::testParseFloatWrongValue() { 
//     string aggStr = "price(range:0,10.0,abc);count()";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseFloatMissValue() { 
//     string aggStr = "price(range:);count()";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseFloatWrongValue2() { 
//     string aggStr = "price(range:0,10.0,10.0);count()";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseFloatWrongValue3() { 
//     string aggStr = "price(range:0,10.0,1.1);count()";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseErrorMaxGroup1() { 
//     string aggStr = "price(maxgroup);count()";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseErrorMaxGroup2() { 
//     string aggStr = "price(maxgroup:);count()";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseErrorMaxGroup3() { 
//     string aggStr = "price(maxgroup:xx);count()";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseErrorMaxGroup4() { 
//     string aggStr = "price(xx);count()";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseErrorMaxGroup5() { 
//     string aggStr = "price(;count()";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseErrorMaxGroup6() { 
//     string aggStr = ";";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseMissKey() { 
//     string aggStr = ";count()";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseOnlyLeftParen() {
//     string aggStr = "(;count()";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseMissRightParen() {
//     string aggStr = "uid(;count()";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseMissKeyNameAndType() {
//     string aggStr = "();count()";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseMissMissKeyName() {
//     string aggStr = "(maxgroup:1000);count()";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseMissSemicolon() {
//     string aggStr = "uid(range);count()";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseMissRangeValue() {
//     string aggStr = "uid(range:);count()";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseRangeWithOnlyComma() {
//     string aggStr = "uid(range:,);count()";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseExtraComma() {
//     string aggStr = "uid(range:1,);count()";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseWrongType() {
//     string aggStr = "uid(xxx);count()";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

// void AggregatorParserTest::testParseWrongRightParen() {
//     string aggStr = "uid(};count()";
//     _agg = _aggParser->parse(aggStr);
//     ASSERT_TRUE(!_agg);
// }

END_HA3_NAMESPACE(search);
