#include <postage_demo/PostageFunction.h>
#include <testlib/function/Ha3FunctionWrap.h>
#include <autil/StringUtil.h>
#include <unittest/unittest.h>
#include <ha3/util/Log.h>

using namespace std;
using namespace autil;
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);

namespace pluginplatform {
namespace function_plugins {

class PostageFunctionTest : public TESTBASE {
public:
    void setUp() {}
    void tearDown() {}
    void createPostageMap(const string &inputStr, PostageFunction::PostageMap &postageMap);
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(function_plugins, PostageFunctionTest);

TEST_F(PostageFunctionTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    PostageFunction::PostageMap postageMap;
    createPostageMap("1,2,1000,200;1,3,2000,4000;2,5,99,200;3,6,200,11;4,2,10,20", postageMap);

    int32_t buyerCity = 2;

    pluginplatform::function_plugins::Ha3FunctionWrap fw(GET_TEST_DATA_PATH(), GET_CLASS_NAME());
    suez::turing::InvertedTableParam param;
    param.addAttribute<int32_t>("seller_city", "1,2,3,4,5,6,7,8");
    param.fillDocs();
    fw.addInvertedTableAndBuildIndex(&param);

    auto sellerCityAttrExpr = dynamic_cast<AttributeExpressionTyped<int32_t> *>(fw.addAttributeExpression<int32_t>("seller_city", "1,2,3,4,5,6,7,8"));

    PostageFunction *function = new PostageFunction(sellerCityAttrExpr, buyerCity, postageMap);
    std::vector<double> expectVals = {1000,20,20,10,20,20,20,20};
    ASSERT_TRUE(fw.callBeginRequest(function));
    ASSERT_TRUE(fw.callEvaluate<double>(function, expectVals));
    auto matchdocs = fw.getMatchDocs();
    std::vector<double> expectDistances = {200,10000,10000,20,10000,10000,10000,10000};
    auto distanceRef = fw._ha3FunctionProvider->findVariableReference<double>("distance");
    for (size_t i = 0; i < matchdocs.size(); ++i) {
        ASSERT_EQ(expectDistances[i], distanceRef->get(matchdocs[i]));
    }
    ASSERT_TRUE(fw.callEndRequest(function));
    delete function;
}

void PostageFunctionTest::createPostageMap(const string &inputStr, 
        PostageFunction::PostageMap &postageMap)
{
    vector<vector<int> > values;
    StringUtil::fromString(inputStr, values, ",", ";");
    for (size_t i = 0; i < values.size(); i++) {
        assert(values[i].size() == 4);
        pair<int, int> key(values[i][0], values[i][1]);
        pair<double, double> value((double)values[i][2], (double)values[i][3]);
        postageMap[key] = value;
    }
}

}}

