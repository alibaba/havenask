#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/LayerClauseValidator.h>
#include <ha3/queryparser/ClauseParserContext.h>

using namespace std;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(queryparser);

BEGIN_HA3_NAMESPACE(qrs);

class LayerClauseValidatorTest : public TESTBASE {
public:
    LayerClauseValidatorTest();
    ~LayerClauseValidatorTest();
public:
    void setUp();
    void tearDown();
protected:
    common::QueryLayerClause *createLayerClause(const std::string &layerClauseStr);
    AttributeInfos* createFakeAttributeInfos(bool createMulti);
protected:
    AttributeInfos *_attrInfos;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, LayerClauseValidatorTest);


#define CHECK_LAYER_CLAUSE_ATTR_NAME(layerCursor, rangeCursor, checkAttrName) \
    {                                                                   \
        LayerDescription *layerDesc = layerClausePtr->getLayerDescription(layerCursor); \
        LayerKeyRange *range = layerDesc->getLayerRange(rangeCursor);   \
        string attrName = range->attrName;                              \
        ASSERT_EQ(checkAttrName, attrName);                  \
    }
    


LayerClauseValidatorTest::LayerClauseValidatorTest() { 
    _attrInfos = NULL;
}

LayerClauseValidatorTest::~LayerClauseValidatorTest() { 
}

void LayerClauseValidatorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}
void LayerClauseValidatorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    DELETE_AND_SET_NULL(_attrInfos);
}

TEST_F(LayerClauseValidatorTest, testSimpleProcess) { 
    _attrInfos = createFakeAttributeInfos(false);
    LayerClauseValidator validator(_attrInfos);
    string layerClauseStr = "range:cat{1,2,3,[10,20],[99,],66,[,44]}*ends{[555,]},quota:1000;"
                            "range:cat{2,3,[10,20],[,]},quota:100;"
                            "range:%other";
    QueryLayerClausePtr layerClausePtr(createLayerClause(layerClauseStr));
    bool ret = validator.validate(layerClausePtr.get());
    (void)ret;
    ASSERT_TRUE(ret);
}

TEST_F(LayerClauseValidatorTest, testCheckRange1) {
    _attrInfos = createFakeAttributeInfos(false);
    LayerClauseValidator validator(_attrInfos);
    string layerClauseStr = "range:%sorted{1}*cat{1,2,3,[10,20],[99,],66,[,44]}*ends{[555,]},quota:1000;"
                            "range:%unsorted{1}*%docid{2,-3,[10,20],[,]},quota:100";
    QueryLayerClausePtr layerClausePtr(createLayerClause(layerClauseStr));
    bool ret = validator.validate(layerClausePtr.get());
    (void)ret;
    ASSERT_TRUE(!ret);
}

TEST_F(LayerClauseValidatorTest, testCheckRange2) {
    _attrInfos = createFakeAttributeInfos(false);
    LayerClauseValidator validator(_attrInfos);
    string layerClauseStr = "range:%sorted{1}*cat{1,2,3,[10,20],[99,],66,[,44]}*ends{[555,]},quota:1000;"
                            "range:%unsorted{1}*%docid{2,3,[10,-20],[,]},quota:100";
    QueryLayerClausePtr layerClausePtr(createLayerClause(layerClauseStr));
    bool ret = validator.validate(layerClausePtr.get());
    (void)ret;
    ASSERT_TRUE(!ret);
}

TEST_F(LayerClauseValidatorTest, testCheckRange3) {
    _attrInfos = createFakeAttributeInfos(false);
    LayerClauseValidator validator(_attrInfos);
    string layerClauseStr = "range:%sorted{1}*%docid{1,2,3,[10,-999999999],[99,],66,[,44]}*ends{[555,]},quota:1000";
    QueryLayerClausePtr layerClausePtr(createLayerClause(layerClauseStr));
    bool ret = validator.validate(layerClausePtr.get());
    (void)ret;
    ASSERT_TRUE(!ret);
}

TEST_F(LayerClauseValidatorTest, testTwoLayerTwoDim) {
    _attrInfos = createFakeAttributeInfos(false);
    LayerClauseValidator validator(_attrInfos);
    string layerClauseStr = "range:%sorted{1}*cat{1,2,3,[10,20],[99,],66,[,44]}*ends{[555,]},quota:1000;"
                            "range:%unsorted{1}*cat{2,3,[10,20],[,]},quota:100";
    QueryLayerClausePtr layerClausePtr(createLayerClause(layerClauseStr));
    bool ret = validator.validate(layerClausePtr.get());
    (void)ret;
    ASSERT_TRUE(ret);
}

TEST_F(LayerClauseValidatorTest, testOneLayer) {
    _attrInfos = createFakeAttributeInfos(false);
    LayerClauseValidator validator(_attrInfos);
    string layerClauseStr = "range:%sorted{1}*cat{1,2,3,[10,20],[99,],66,[,44]}*ends{[555,]},quota:1000";
    QueryLayerClausePtr layerClausePtr(createLayerClause(layerClauseStr));
    bool ret = validator.validate(layerClausePtr.get());
    (void)ret;
    ASSERT_TRUE(ret);
}

TEST_F(LayerClauseValidatorTest, testTwoLayerThreeDim) {
    _attrInfos = createFakeAttributeInfos(false);
    LayerClauseValidator validator(_attrInfos);
    string layerClauseStr = "range:%sorted{1}*%percent{1}*cat{1,2,3,[10,20],[99,],66,[,44]}*ends{[555,]},quota:1000;"
                            "range:%unsorted{1}*cat{2,3,[10,20],[,]},quota:100";
    QueryLayerClausePtr layerClausePtr(createLayerClause(layerClauseStr));
    bool ret = validator.validate(layerClausePtr.get());
    (void)ret;
    ASSERT_TRUE(ret);
}


TEST_F(LayerClauseValidatorTest, testTwoLayerTwoDimThreeAttr) {
    _attrInfos = createFakeAttributeInfos(false);
    LayerClauseValidator validator(_attrInfos);
    string layerClauseStr = "range:%sorted{1}*%percent{1}*cat{1,2,3,[10,20],[99,],66,[,44]}*ends{[555,]}*attribute{1,2},quota:1000;"
                            "range:%unsorted{1}*cat{2,3,[10,20],[,]},quota:100";
    QueryLayerClausePtr layerClausePtr(createLayerClause(layerClauseStr));
    bool ret = validator.validate(layerClausePtr.get());
    (void)ret;
    ASSERT_TRUE(ret);
}

TEST_F(LayerClauseValidatorTest, testTwoLayerTwoDimThreeAttrNoneKeyWord) {
    _attrInfos = createFakeAttributeInfos(false);
    LayerClauseValidator validator(_attrInfos);
    string layerClauseStr = "range:%use_sorted{1}*%percent{1}*cat{1,2,3,[10,20],[99,],66,[,44]}*ends{[555,]}*attribute{1,2},quota:1000;"
                            "range:%unsorted{1}*cat{2,3,[10,20],[,]},quota:100";
    QueryLayerClausePtr layerClausePtr(createLayerClause(layerClauseStr));
    bool ret = validator.validate(layerClausePtr.get());
    (void)ret;
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_ATTRIBUTE_NOT_EXIST, validator.getErrorCode());
}

TEST_F(LayerClauseValidatorTest, testTwoLayerTwoDimThreeAttrNoneAttr) {
    _attrInfos = createFakeAttributeInfos(false);
    LayerClauseValidator validator(_attrInfos);
    string layerClauseStr = "range:%sorted{1}*%percent{1}*not_exist_attr{1,2,3,[10,20],[99,],66,[,44]}*ends{[555,]}*attribute{1,2},quota:1000;"
                            "range:%unsorted{1}*cat{2,3,[10,20],[,]},quota:100";
    QueryLayerClausePtr layerClausePtr(createLayerClause(layerClauseStr));
    bool ret = validator.validate(layerClausePtr.get());
    (void)ret;
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_ATTRIBUTE_NOT_EXIST, validator.getErrorCode());
}

TEST_F(LayerClauseValidatorTest, testNull) {
    _attrInfos = createFakeAttributeInfos(false);
    LayerClauseValidator validator(_attrInfos);
    bool ret = validator.validate(NULL);
    (void)ret;
    ASSERT_TRUE(ret);
}

TEST_F(LayerClauseValidatorTest, testSequence) {
    _attrInfos = createFakeAttributeInfos(false);
    LayerClauseValidator validator(_attrInfos);
    string layerClauseStr = "range:%sorted{1}*cat{1,2,3,[10,20],[99,],66,[,44]}*ends{[555,]}*%percent{1}*attribute{1,2},quota:1000;"
                            "range:%unsorted{1}*cat{2,3,[10,20],[,]}*%percent{1},quota:100";
    QueryLayerClausePtr layerClausePtr(createLayerClause(layerClauseStr));
    bool ret = validator.validate(layerClausePtr.get());
    (void)ret;
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_LAYER_CLAUSE_ATTRIBUTE_NOT_SEQUENCE, validator.getErrorCode());    
}

TEST_F(LayerClauseValidatorTest, testAutoAddSortedRange) { 
    _attrInfos = createFakeAttributeInfos(false);
    LayerClauseValidator validator(_attrInfos);
    string layerClauseStr = "range:cat{66}*ends{[555,]},quota:1000;"
                            "range:cat{2,3},quota:100";
    QueryLayerClausePtr layerClausePtr(createLayerClause(layerClauseStr));
    bool ret = validator.validate(layerClausePtr.get());
    (void)ret;
    ASSERT_TRUE(ret);
    CHECK_LAYER_CLAUSE_ATTR_NAME(0, 0, LAYERKEY_SORTED);
    CHECK_LAYER_CLAUSE_ATTR_NAME(1, 0, LAYERKEY_SORTED);
}

TEST_F(LayerClauseValidatorTest, testAutoAddSortedRangeWithNoAttr) { 
    _attrInfos = createFakeAttributeInfos(false);
    LayerClauseValidator validator(_attrInfos);
    string layerClauseStr = "range:%other{1},quota:1000;"
                            "range:%percent{1},quota:100";
    QueryLayerClausePtr layerClausePtr(createLayerClause(layerClauseStr));
    bool ret = validator.validate(layerClausePtr.get());
    (void)ret;
    ASSERT_TRUE(ret);
    CHECK_LAYER_CLAUSE_ATTR_NAME(0, 0, LAYERKEY_OTHER);
    CHECK_LAYER_CLAUSE_ATTR_NAME(1, 0, LAYERKEY_PERCENT);
}

TEST_F(LayerClauseValidatorTest, testAutoAddSortedRangeWithAllSorted) { 
    _attrInfos = createFakeAttributeInfos(false);
    LayerClauseValidator validator(_attrInfos);
    string layerClauseStr = "range:%sorted{1}*cat{66}*ends{[555,]},quota:1000;"
                            "range:%sorted{1}*cat{2,3},quota:100";
    QueryLayerClausePtr layerClausePtr(createLayerClause(layerClauseStr));
    bool ret = validator.validate(layerClausePtr.get());
    (void)ret;
    ASSERT_TRUE(ret);
    CHECK_LAYER_CLAUSE_ATTR_NAME(0, 0, LAYERKEY_SORTED);
    CHECK_LAYER_CLAUSE_ATTR_NAME(0, 1, string("cat"));
    CHECK_LAYER_CLAUSE_ATTR_NAME(0, 2, string("ends"));
    CHECK_LAYER_CLAUSE_ATTR_NAME(1, 0, LAYERKEY_SORTED);
    CHECK_LAYER_CLAUSE_ATTR_NAME(1, 1, string("cat"));
}

TEST_F(LayerClauseValidatorTest, testAutoAddSortedRangeWithUnsorted) { 
    _attrInfos = createFakeAttributeInfos(false);
    LayerClauseValidator validator(_attrInfos);
    string layerClauseStr = "range:%unsorted*cat{66},quota:1000;"
                            "range:%sorted*cat{2,3},quota:100";
    QueryLayerClausePtr layerClausePtr(createLayerClause(layerClauseStr));
    bool ret = validator.validate(layerClausePtr.get());
    (void)ret;
    ASSERT_TRUE(ret);
    CHECK_LAYER_CLAUSE_ATTR_NAME(0, 0, LAYERKEY_UNSORTED);
    CHECK_LAYER_CLAUSE_ATTR_NAME(0, 1, LAYERKEY_SORTED);
    CHECK_LAYER_CLAUSE_ATTR_NAME(0, 2, string("cat"));
    CHECK_LAYER_CLAUSE_ATTR_NAME(1, 0, LAYERKEY_SORTED);
    CHECK_LAYER_CLAUSE_ATTR_NAME(1, 1, string("cat"));
}

TEST_F(LayerClauseValidatorTest, testAutoAddSortedRangeWithUnsortedNormal) { 
    _attrInfos = createFakeAttributeInfos(false);
    LayerClauseValidator validator(_attrInfos);
    string layerClauseStr = "range:%unsorted,quota:1000;"
                            "range:%sorted*cat{2,3},quota:100";
    QueryLayerClausePtr layerClausePtr(createLayerClause(layerClauseStr));
    bool ret = validator.validate(layerClausePtr.get());
    (void)ret;
    ASSERT_TRUE(ret);
    CHECK_LAYER_CLAUSE_ATTR_NAME(0, 0, LAYERKEY_UNSORTED);
    CHECK_LAYER_CLAUSE_ATTR_NAME(1, 0, LAYERKEY_SORTED);
    CHECK_LAYER_CLAUSE_ATTR_NAME(1, 1, string("cat"));
}

QueryLayerClause *LayerClauseValidatorTest::createLayerClause(const string &layerClauseStr) {
    ClauseParserContext ctx;
    bool ret = ctx.parseLayerClause(layerClauseStr.c_str());
    (void)ret;
    assert(ret);
    return ctx.stealLayerClause();
}


AttributeInfos *LayerClauseValidatorTest::createFakeAttributeInfos(bool createMulti) {
    AttributeInfos *attrInfos = new AttributeInfos;

#define ADD_ATTR_TYPE_HELPER(name, ft_type, isMulti, isSub)     \
    do {                                                        \
        AttributeInfo *attrInfo = new AttributeInfo();          \
        attrInfo->setAttrName(name);                            \
        attrInfo->setMultiValueFlag(isMulti);                   \
        attrInfo->setSubDocAttributeFlag(isSub);                \
        FieldInfo fieldInfo(name, ft_type);                     \
        attrInfo->setFieldInfo(fieldInfo);                      \
        attrInfos->addAttributeInfo(attrInfo);                  \
    } while(0)

    ADD_ATTR_TYPE_HELPER("attribute", ft_integer, false, false);
    ADD_ATTR_TYPE_HELPER("ends", ft_integer, false, false);
    ADD_ATTR_TYPE_HELPER("cat", ft_integer, false, false);
    if (createMulti) {
        ADD_ATTR_TYPE_HELPER("multi_attr_1", ft_integer, true, false);
        ADD_ATTR_TYPE_HELPER("multi_attr_2", ft_integer, true, false);
        ADD_ATTR_TYPE_HELPER("multi_string_attr", ft_string, true, false);
        }
    

#undef ADD_ATTR_TYPE_HELPER

    return attrInfos;
}

END_HA3_NAMESPACE(qrs);

