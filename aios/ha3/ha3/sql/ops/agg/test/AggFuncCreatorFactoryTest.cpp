#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/sql/ops/agg/AggFuncCreatorFactory.h>
#include <ha3/sql/ops/agg/builtin/CountAggFunc.h>

using namespace std;
using namespace testing;

BEGIN_HA3_NAMESPACE(sql);

class FackFactory : public AggFuncCreatorFactory {
    bool registerAggFunctions() override {
        return true;
    }
    bool registerAggFunctionInfos() override {
        return true;
    }
};

class AggFuncCreatorFactoryTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    FackFactory *_aggFuncCreatorFactory;
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(agg, AggFuncCreatorFactoryTest);

void AggFuncCreatorFactoryTest::setUp() {
    _aggFuncCreatorFactory = new FackFactory();
}

void AggFuncCreatorFactoryTest::tearDown() {
    delete _aggFuncCreatorFactory;
}

TEST_F(AggFuncCreatorFactoryTest, testRegisterAggFunc) {
    CountAggFuncCreator *creator = nullptr;
    ASSERT_TRUE(_aggFuncCreatorFactory->registerAggFunction("count", creator));
    ASSERT_EQ(1, _aggFuncCreatorFactory->getAggFuncCreators().size());
    ASSERT_FALSE(_aggFuncCreatorFactory->registerAggFunction("count", creator));
    ASSERT_EQ(1, _aggFuncCreatorFactory->getAggFuncCreators().size());
    ASSERT_TRUE(_aggFuncCreatorFactory->registerAggFunction("count2", creator));
    ASSERT_EQ(2, _aggFuncCreatorFactory->getAggFuncCreators().size());
}

TEST_F(AggFuncCreatorFactoryTest, testGetAggFuncCreator) {
    CountAggFuncCreator *creator1 = new CountAggFuncCreator();
    CountAggFuncCreator *creator2 = new CountAggFuncCreator();
    ASSERT_TRUE(_aggFuncCreatorFactory->registerAggFunction("count", creator1));
    ASSERT_TRUE(_aggFuncCreatorFactory->registerAggFunction("count2", creator2));
    ASSERT_EQ(creator1, _aggFuncCreatorFactory->getAggFuncCreator("count"));
    ASSERT_EQ(creator2, _aggFuncCreatorFactory->getAggFuncCreator("count2"));
    ASSERT_TRUE(_aggFuncCreatorFactory->getAggFuncCreator("count3") == nullptr);
}

TEST_F(AggFuncCreatorFactoryTest, testGetAggFuncCreators) {
    CountAggFuncCreator *creator1 = new CountAggFuncCreator();;
    CountAggFuncCreator *creator2 = new CountAggFuncCreator();;
    ASSERT_TRUE(_aggFuncCreatorFactory->registerAggFunction("count", creator1));
    ASSERT_TRUE(_aggFuncCreatorFactory->registerAggFunction("count2", creator2));
    auto aggFuncCreatorMap = _aggFuncCreatorFactory->getAggFuncCreators();
    ASSERT_EQ(2, aggFuncCreatorMap.size());
    ASSERT_EQ(creator1, aggFuncCreatorMap["count"]);
    ASSERT_EQ(creator2, aggFuncCreatorMap["count2"]);
}

TEST_F(AggFuncCreatorFactoryTest, testParseIquanParamDef) {
    std::vector<iquan::ParamTypeDef> paramsDef;
    ASSERT_FALSE(AggFuncCreatorFactory::parseIquanParamDef("int32, int64", paramsDef));
    ASSERT_FALSE(AggFuncCreatorFactory::parseIquanParamDef("{int32, int64}", paramsDef));
    ASSERT_TRUE(AggFuncCreatorFactory::parseIquanParamDef("[int32, int64]", paramsDef));
    ASSERT_EQ(2, paramsDef.size());
    ASSERT_EQ(false, paramsDef[0].isMulti);
    ASSERT_EQ("int32", paramsDef[0].type);
    ASSERT_EQ(false, paramsDef[1].isMulti);
    ASSERT_EQ("int64", paramsDef[1].type);
    ASSERT_TRUE(AggFuncCreatorFactory::parseIquanParamDef("[int32 int64]", paramsDef));
    ASSERT_EQ(3, paramsDef.size());
    ASSERT_EQ(false, paramsDef[2].isMulti);
    ASSERT_EQ("int32 int64", paramsDef[2].type);
    paramsDef.clear();
    ASSERT_TRUE(AggFuncCreatorFactory::parseIquanParamDef("[  Mint32 , double ]", paramsDef));
    ASSERT_EQ(2, paramsDef.size());
    ASSERT_EQ(true, paramsDef[0].isMulti);
    ASSERT_EQ("array", paramsDef[0].type);
    ASSERT_EQ(false, paramsDef[1].isMulti);
    ASSERT_EQ("double", paramsDef[1].type);
    ASSERT_FALSE(AggFuncCreatorFactory::parseIquanParamDef("[  , double ]", paramsDef));
    ASSERT_FALSE(AggFuncCreatorFactory::parseIquanParamDef("", paramsDef));
    paramsDef.clear();
    ASSERT_TRUE(AggFuncCreatorFactory::parseIquanParamDef("[]", paramsDef));
    ASSERT_EQ(0, paramsDef.size());
}

TEST_F(AggFuncCreatorFactoryTest, testRegisterAggFuncInfo) {
    ASSERT_TRUE(_aggFuncCreatorFactory->registerAggFuncInfo(
                    "sum", "[int64]", "[int32]", "[double, Mint32]"));
    ASSERT_EQ(1, _aggFuncCreatorFactory->_accSize.size());
    ASSERT_EQ(1, _aggFuncCreatorFactory->_accSize["sum"]);
    ASSERT_EQ(1, _aggFuncCreatorFactory->_functionInfosDef.functions.size());
    const auto &def = _aggFuncCreatorFactory->_functionInfosDef.functions[0];
    ASSERT_EQ("sum", def.functionName);
    ASSERT_EQ(1, def.isDeterministic);
    ASSERT_EQ("UDAF", def.functionType);
    ASSERT_EQ("json_default_0.1", def.functionContentVersion);

    ASSERT_EQ(1, def.functionContent.prototypes.size());
    ASSERT_EQ(1, def.functionContent.prototypes[0].paramTypes.size());
    ASSERT_EQ(false, def.functionContent.prototypes[0].paramTypes[0].isMulti);
    ASSERT_EQ("int32", def.functionContent.prototypes[0].paramTypes[0].type);
    ASSERT_EQ(2, def.functionContent.prototypes[0].returnTypes.size());
    ASSERT_EQ(false, def.functionContent.prototypes[0].returnTypes[0].isMulti);
    ASSERT_EQ("double", def.functionContent.prototypes[0].returnTypes[0].type);
    ASSERT_EQ(true, def.functionContent.prototypes[0].returnTypes[1].isMulti);
    ASSERT_EQ("array", def.functionContent.prototypes[0].returnTypes[1].type);
    ASSERT_EQ(1, def.functionContent.prototypes[0].accTypes.size());
    ASSERT_EQ(false, def.functionContent.prototypes[0].accTypes[0].isMulti);
    ASSERT_EQ("int64", def.functionContent.prototypes[0].accTypes[0].type);
    ASSERT_FALSE(_aggFuncCreatorFactory->registerAggFuncInfo(
                    "sum1", "", "[,]", "[double, Mint32]"));
    ASSERT_FALSE(_aggFuncCreatorFactory->registerAggFuncInfo(
                    "sum2", "[int32]", "", "[double, Mint32]"));
    ASSERT_FALSE(_aggFuncCreatorFactory->registerAggFuncInfo(
                    "sum3", "[int32]", "[double]", ""));
    ASSERT_TRUE(_aggFuncCreatorFactory->registerAggFuncInfo(
                    "sum4", "[int32]", "[double]", "[double, Mint32]"));
    ASSERT_EQ(2, _aggFuncCreatorFactory->_functionInfosDef.functions.size());
}

TEST_F(AggFuncCreatorFactoryTest, testMultiParamsRegisterAggFuncInfo) {
    ASSERT_TRUE(_aggFuncCreatorFactory->registerAggFuncInfo(
                    "JoinSeriesDataAgg", "[string]|[string]", "[int64, double]|[int64, int64]", "[string]|[string]"));
    ASSERT_EQ(1, _aggFuncCreatorFactory->_accSize.size());
    ASSERT_EQ(1, _aggFuncCreatorFactory->_accSize["JoinSeriesDataAgg"]);
    ASSERT_EQ(1, _aggFuncCreatorFactory->_functionInfosDef.functions.size());
    const auto &def = _aggFuncCreatorFactory->_functionInfosDef.functions[0];
    ASSERT_EQ("JoinSeriesDataAgg", def.functionName);
    ASSERT_EQ(1, def.isDeterministic);
    ASSERT_EQ("UDAF", def.functionType);
    ASSERT_EQ("json_default_0.1", def.functionContentVersion);
    ASSERT_EQ(2, def.functionContent.prototypes.size());
    ASSERT_EQ(2, def.functionContent.prototypes[0].paramTypes.size());
    ASSERT_EQ(false, def.functionContent.prototypes[0].paramTypes[0].isMulti);
    ASSERT_EQ("int64", def.functionContent.prototypes[0].paramTypes[0].type);
    ASSERT_EQ(false, def.functionContent.prototypes[0].paramTypes[1].isMulti);
    ASSERT_EQ("double", def.functionContent.prototypes[0].paramTypes[1].type);
    ASSERT_EQ(1, def.functionContent.prototypes[0].returnTypes.size());
    ASSERT_EQ(false, def.functionContent.prototypes[0].returnTypes[0].isMulti);
    ASSERT_EQ("string", def.functionContent.prototypes[0].returnTypes[0].type);
    ASSERT_EQ(1, def.functionContent.prototypes[0].accTypes.size());
    ASSERT_EQ(false, def.functionContent.prototypes[0].accTypes[0].isMulti);
    ASSERT_EQ("string", def.functionContent.prototypes[0].accTypes[0].type);

    ASSERT_EQ(2, def.functionContent.prototypes[1].paramTypes.size());
    ASSERT_EQ(false, def.functionContent.prototypes[1].paramTypes[0].isMulti);
    ASSERT_EQ("int64", def.functionContent.prototypes[1].paramTypes[0].type);
    ASSERT_EQ(false, def.functionContent.prototypes[1].paramTypes[1].isMulti);
    ASSERT_EQ("int64", def.functionContent.prototypes[1].paramTypes[1].type);
    ASSERT_EQ(1, def.functionContent.prototypes[1].returnTypes.size());
    ASSERT_EQ(false, def.functionContent.prototypes[1].returnTypes[0].isMulti);
    ASSERT_EQ("string", def.functionContent.prototypes[1].returnTypes[0].type);
    ASSERT_EQ(1, def.functionContent.prototypes[1].accTypes.size());
    ASSERT_EQ(false, def.functionContent.prototypes[1].accTypes[0].isMulti);
    ASSERT_EQ("string", def.functionContent.prototypes[1].accTypes[0].type);


    ASSERT_FALSE(_aggFuncCreatorFactory->registerAggFuncInfo(
                    "JoinSeriesDataAgg", "[string]", "[int64, int64]", "[string]|[string]"));
    ASSERT_FALSE(_aggFuncCreatorFactory->registerAggFuncInfo(
                    "JoinSeriesDataAgg", "[string]", "[int64, double]|[int64, int64]", "[string]"));
    ASSERT_FALSE(_aggFuncCreatorFactory->registerAggFuncInfo(
                    "JoinSeriesDataAgg", "[string]|[string]", "[int64, double]", "[string]"));

    ASSERT_EQ(1, _aggFuncCreatorFactory->_functionInfosDef.functions.size());
}

END_HA3_NAMESPACE();
