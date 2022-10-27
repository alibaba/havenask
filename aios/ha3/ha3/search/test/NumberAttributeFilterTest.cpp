#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/util/AttributeInfos.h>
#include <suez/turing/expression/util/FieldInfos.h>
#include <ha3/index/AttributeReader.h>
#include <ha3/index/IntegerAttributeReader.h>
#include <ha3/index/AttributeReaderFactory.h>
#include <ha3/search/NumberAttributeFilter.h>
#include <ha3/index/SegmentInfoReader.h>
#include <ha3/test/test.h>
#include <string>

using namespace std;
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(index);

BEGIN_HA3_NAMESPACE(search);
class AttributeReader;
class NumberAttributeFilter;
class NumberAttributeFilterTest : public TESTBASE {
public:
    NumberAttributeFilterTest();
    ~NumberAttributeFilterTest();
public:
    void setUp();
    void tearDown();
protected:
    config::AttributeInfo *_attrInfo;
    config::FieldInfo *_fieldInfo;
    index::AttributeReader *_attribReader;
    NumberAttributeFilter *_filter;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, NumberAttributeFilterTest);


NumberAttributeFilterTest::NumberAttributeFilterTest() { 
    _filter = NULL;
    _attribReader = NULL;
    _attrInfo = NULL;
    _fieldInfo = NULL;
}

NumberAttributeFilterTest::~NumberAttributeFilterTest() { 
}

void NumberAttributeFilterTest::setUp() { 
    HA3_LOG(TRACE3, "setUp");
    string basePath = TEST_DATA_PATH;
    basePath += "/newnumberdbroot/simple";
    string segInfosPath = basePath + "/SegmentInfos.segs";

    string attriPath = basePath + "/attribute";
    _attrInfo = new AttributeInfo(attriPath.c_str());
    _attrInfo->setAttrName("category_id");
    _fieldInfo = new FieldInfo();
    _fieldInfo->fieldType = ft_integer;
    _attrInfo->setFieldInfo(_fieldInfo);

    SegmentInfoReader segInfoReader;
    bool ret = segInfoReader.load(segInfosPath.c_str());
    ASSERT_TRUE(ret);
    SegmentInfos segmentInfos = segInfoReader.getSegmentInfos();

    _attribReader = 
        AttributeReaderFactory::createAttributeReader(_attrInfo, segmentInfos);
    ASSERT_TRUE(_attribReader != NULL);
    _filter = new NumberAttributeFilter(dynamic_cast<IntegerAttributeReader*>(_attribReader));
}

void NumberAttributeFilterTest::tearDown() { 
    delete _attribReader;
    delete _filter;
    delete _attrInfo;
    delete _fieldInfo;
}

TEST_F(NumberAttributeFilterTest, testFilterNotFiltered) {
    HA3_LOG(DEBUG, "Begin Test!");
    NumberRange numRange;
    numRange.min = 1996;
    numRange.max = 3829;
    _filter->setRange(numRange);
    bool ret = _filter->filter(55);
    ASSERT_TRUE(!ret);
}

TEST_F(NumberAttributeFilterTest, testFilterFiltered) {
    HA3_LOG(DEBUG, "Begin Test!");
    NumberRange numRange;
    numRange.min = 1996;
    numRange.max = 3829;
    _filter->setRange(numRange);
    bool ret = _filter->filter(34);
    ASSERT_TRUE(ret);
}

TEST_F(NumberAttributeFilterTest, testFilterLeftSide) {
    HA3_LOG(DEBUG, "Begin Test!");
    NumberRange numRange;
    numRange.min = 1996;
    numRange.max = 3829;
    _filter->setRange(numRange);
    bool ret = _filter->filter(79);
    ASSERT_TRUE(!ret);
}

TEST_F(NumberAttributeFilterTest, testFilterRight) { 
    HA3_LOG(DEBUG, "Begin Test!");
    NumberRange numRange;
    numRange.min = 1996;
    numRange.max = 3829;
    _filter->setRange(numRange);
    bool ret = _filter->filter(8);
    ASSERT_TRUE(!ret);
}

TEST_F(NumberAttributeFilterTest, testFilterWrongDocId) {
    HA3_LOG(DEBUG, "Begin Test!");
    NumberRange numRange;
    numRange.min = 1996;
    numRange.max = 3829;
    _filter->setRange(numRange);
    bool ret = _filter->filter(101);
    ASSERT_TRUE(ret);
}

END_HA3_NAMESPACE(search);

