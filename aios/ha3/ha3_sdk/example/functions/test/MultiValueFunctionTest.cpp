#include <ha3_sdk/example/functions/MultiValueFunction.h>
#include <ha3_sdk/testlib/function/Ha3FunctionWrap.h>
#include <unittest/unittest.h>
#include <ha3/util/Log.h>
#include "autil/StringUtil.h"
#include "autil/MultiValueCreator.h"

using namespace std;
USE_HA3_NAMESPACE(search);
BEGIN_HA3_NAMESPACE(func_expression);

class MultiValueFunctionTest : public TESTBASE {
public:
    void setUp() {}
    void tearDown() {}
    HA3_LOG_DECLARE();
private:
    autil::mem_pool::Pool _pool;
};

HA3_LOG_SETUP(functions, MultiValueFunctionTest);

TEST_F(MultiValueFunctionTest, testMultiInt) {
    HA3_LOG(DEBUG, "Begin Test!");

    HA3_NS(function)::Ha3FunctionWrap fw(GET_TEST_DATA_PATH(), GET_CLASS_NAME());
    suez::turing::InvertedTableParam param;

    string attrData = "1,2#2,3#3,4";
    typedef autil::MultiValueType<int> MultiInt;

    param.addMultiValAttribute<int>("attr1", attrData);
    param.fillDocs();
    fw.addInvertedTableAndBuildIndex(&param);

    auto multiIntAttrExpr = dynamic_cast<AttributeExpressionTyped<MultiInt> *>(fw.addMultiValAttributeExpression<int>("attr1", attrData));

    vector<MultiInt> expectVals;
    vector<vector<int>> values;
    fw.makeMultiValue(attrData, expectVals, values);
    MultiValueFunction<int> *func = new MultiValueFunction<int>(multiIntAttrExpr);
    ASSERT_TRUE(fw.callBeginRequest(func));
    ASSERT_TRUE(fw.callEvaluate<MultiInt>(func, expectVals));
    
    delete func;
}

TEST_F(MultiValueFunctionTest, testString) {
    HA3_NS(function)::Ha3FunctionWrap fw(GET_TEST_DATA_PATH(), GET_CLASS_NAME());
    suez::turing::InvertedTableParam param;

    string attrData = "abc,def,ghi,jkl";
    typedef autil::MultiValueType<char> MultiChar;

    param.addAttribute<string>("attr1", attrData);
    param.fillDocs();
    fw.addInvertedTableAndBuildIndex(&param);

    auto stringAttrExpr = dynamic_cast<AttributeExpressionTyped<MultiChar> *>(fw.addAttributeExpression<MultiChar>("attr1", attrData));

    MultiValueFunction<char> *func = new MultiValueFunction<char>(stringAttrExpr);

    vector<string> tmp = autil::StringUtil::split(attrData, ",");
    vector<MultiChar> expectVals;
    for (auto s : tmp) {
        MultiChar mc(autil::MultiValueCreator::createMultiValueBuffer(s.data(), s.size(), &_pool));
        expectVals.push_back(mc);
    }

    ASSERT_TRUE(fw.callBeginRequest(func));
    ASSERT_TRUE(fw.callEvaluate<MultiChar>(func, expectVals));

    delete func;
}

TEST_F(MultiValueFunctionTest, testMultiString) {
    HA3_NS(function)::Ha3FunctionWrap fw(GET_TEST_DATA_PATH(), GET_CLASS_NAME());
    suez::turing::InvertedTableParam param;

    string attrData = "abc,def#ghi,jkl";
    typedef autil::MultiValueType<char> MultiChar;
    typedef autil::MultiValueType<MultiChar> MultiString;

    param.addMultiValAttribute<string>("attr1", attrData);
    param.fillDocs();
    fw.addInvertedTableAndBuildIndex(&param);

    auto multiStringAttrExpr = dynamic_cast<AttributeExpressionTyped<MultiString> *>(
            fw.addMultiValAttributeExpression<MultiChar>("attr1", attrData));

    MultiValueFunction<MultiChar> *func =
        new MultiValueFunction<MultiChar>(multiStringAttrExpr);

    vector<MultiString> expectVals;
    vector<vector<string>> values;
    autil::StringUtil::fromString(attrData, values, ",", "#");

    for (size_t i = 0; i < values.size(); ++i) {
        MultiString ms(autil::MultiValueCreator::createMultiValueBuffer(values[i], &_pool));
        expectVals.push_back(ms);
    }

    ASSERT_TRUE(fw.callBeginRequest(func));
    ASSERT_TRUE(fw.callEvaluate<MultiString>(func, expectVals));

    delete func;
}

END_HA3_NAMESPACE(func_expression);
