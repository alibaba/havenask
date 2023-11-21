#include "sql/ops/agg/AggFuncCreatorR.h"

#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/legacy/json.h"
#include "iquan/common/catalog/FunctionCommonDef.h"
#include "iquan/common/catalog/FunctionDef.h"
#include "iquan/common/catalog/FunctionModel.h"
#include "matchdoc/ValueType.h"
#include "navi/common.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/ops/agg/AggFunc.h"
#include "sql/ops/agg/AggFuncFactoryR.h"
#include "sql/ops/agg/AggFuncMode.h"
#include "unittest/unittest.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

using namespace testing;

namespace sql {

class MockAggFuncCreator : public AggFuncCreatorR {
public:
    MOCK_METHOD1(init, navi::ErrorCode(navi::ResourceInitContext &));
    MOCK_CONST_METHOD0(getResourceName, std::string());
    MOCK_CONST_METHOD0(getAggFuncName, std::string());
    MOCK_METHOD3(createLocalFunction,
                 AggFunc *(const std::vector<table::ValueType> &inputTypes,
                           const std::vector<std::string> &inputFields,
                           const std::vector<std::string> &outputFields));
    MOCK_METHOD3(createGlobalFunction,
                 AggFunc *(const std::vector<table::ValueType> &inputTypes,
                           const std::vector<std::string> &inputFields,
                           const std::vector<std::string> &outputFields));
    MOCK_METHOD3(createNormalFunction,
                 AggFunc *(const std::vector<table::ValueType> &inputTypes,
                           const std::vector<std::string> &inputFields,
                           const std::vector<std::string> &outputFields));
};

class AggFuncCreatorRTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void AggFuncCreatorRTest::setUp() {}

void AggFuncCreatorRTest::tearDown() {}

TEST_F(AggFuncCreatorRTest, testParseIquanParamDef) {
    std::vector<iquan::ParamTypeDef> paramsDef;
    ASSERT_FALSE(AggFuncCreatorR::parseIquanParamDef("int32, int64", paramsDef));
    ASSERT_FALSE(AggFuncCreatorR::parseIquanParamDef("{int32, int64}", paramsDef));
    ASSERT_TRUE(AggFuncCreatorR::parseIquanParamDef("[int32, int64]", paramsDef));
    ASSERT_EQ(2, paramsDef.size());
    ASSERT_TRUE(!paramsDef[0].isMulti);
    ASSERT_EQ("int32", paramsDef[0].type);
    ASSERT_TRUE(!paramsDef[1].isMulti);
    ASSERT_EQ("int64", paramsDef[1].type);
    ASSERT_TRUE(AggFuncCreatorR::parseIquanParamDef("[int32 int64]", paramsDef));
    ASSERT_EQ(3, paramsDef.size());
    ASSERT_TRUE(!paramsDef[2].isMulti);
    ASSERT_EQ("int32 int64", paramsDef[2].type);
    paramsDef.clear();
    ASSERT_TRUE(AggFuncCreatorR::parseIquanParamDef("[  Mint32 , double ]", paramsDef));
    ASSERT_EQ(2, paramsDef.size());
    ASSERT_TRUE(paramsDef[0].isMulti);
    ASSERT_EQ("array", paramsDef[0].type);
    ASSERT_TRUE(!paramsDef[1].isMulti);
    ASSERT_EQ("double", paramsDef[1].type);
    ASSERT_FALSE(AggFuncCreatorR::parseIquanParamDef("[  , double ]", paramsDef));
    ASSERT_FALSE(AggFuncCreatorR::parseIquanParamDef("", paramsDef));
    paramsDef.clear();
    ASSERT_TRUE(AggFuncCreatorR::parseIquanParamDef("[]", paramsDef));
    ASSERT_EQ(0, paramsDef.size());
}

TEST_F(AggFuncCreatorRTest, testRegisterAggFuncInfo) {
    autil::legacy::json::JsonMap properties;
    {
        MockAggFuncCreator mockCreator;
        EXPECT_CALL(mockCreator, getAggFuncName()).WillOnce(Return(std::string("mock_sum")));
        ASSERT_TRUE(
            mockCreator.initFunctionModel("[int64]", "[int32]", "[double, Mint32]", properties));
        const auto &def = mockCreator._functionModel;
        ASSERT_EQ("mock_sum", def.functionName);
        ASSERT_EQ(1, def.isDeterministic);
        ASSERT_EQ("UDAF", def.functionType);
        ASSERT_EQ("json_default_0.1", def.functionContentVersion);

        ASSERT_EQ(1, def.functionDef.prototypes.size());
        ASSERT_EQ(1, def.functionDef.prototypes[0].paramTypes.size());
        ASSERT_TRUE(!def.functionDef.prototypes[0].paramTypes[0].isMulti);
        ASSERT_EQ("int32", def.functionDef.prototypes[0].paramTypes[0].type);
        ASSERT_EQ(2, def.functionDef.prototypes[0].returnTypes.size());
        ASSERT_TRUE(!def.functionDef.prototypes[0].returnTypes[0].isMulti);
        ASSERT_EQ("double", def.functionDef.prototypes[0].returnTypes[0].type);
        ASSERT_TRUE(def.functionDef.prototypes[0].returnTypes[1].isMulti);
        ASSERT_EQ("array", def.functionDef.prototypes[0].returnTypes[1].type);
        ASSERT_EQ(1, def.functionDef.prototypes[0].accTypes.size());
        ASSERT_TRUE(!def.functionDef.prototypes[0].accTypes[0].isMulti);
        ASSERT_EQ("int64", def.functionDef.prototypes[0].accTypes[0].type);
    }
    {
        MockAggFuncCreator mockCreator;
        EXPECT_CALL(mockCreator, getAggFuncName()).WillOnce(Return(std::string("sum1")));
        ASSERT_FALSE(mockCreator.initFunctionModel("", "[,]", "[double, Mint32]", properties));
    }
    {
        MockAggFuncCreator mockCreator;
        EXPECT_CALL(mockCreator, getAggFuncName()).WillOnce(Return(std::string("sum1")));
        ASSERT_FALSE(mockCreator.initFunctionModel("[int32]", "", "[double, Mint32]", properties));
    }
    {
        MockAggFuncCreator mockCreator;
        EXPECT_CALL(mockCreator, getAggFuncName()).WillOnce(Return(std::string("sum1")));
        ASSERT_FALSE(mockCreator.initFunctionModel("[int32]", "[double]", "", properties));
    }
    {
        MockAggFuncCreator mockCreator;
        EXPECT_CALL(mockCreator, getAggFuncName()).WillOnce(Return(std::string("sum1")));
        ASSERT_TRUE(
            mockCreator.initFunctionModel("[int32]", "[double]", "[double, Mint32]", properties));
    }
}

TEST_F(AggFuncCreatorRTest, testMultiParamsRegisterAggFuncInfo) {
    autil::legacy::json::JsonMap properties;
    {
        MockAggFuncCreator mockCreator;
        EXPECT_CALL(mockCreator, getAggFuncName())
            .WillOnce(Return(std::string("JoinSeriesDataAgg")));
        ASSERT_TRUE(mockCreator.initFunctionModel("[string]|[string]",
                                                  "[int64, double]|[int64, int64]",
                                                  "[string]|[string]",
                                                  properties));
        const auto &def = mockCreator._functionModel;
        ASSERT_EQ("JoinSeriesDataAgg", def.functionName);
        ASSERT_EQ(1, def.isDeterministic);
        ASSERT_EQ("UDAF", def.functionType);
        ASSERT_EQ("json_default_0.1", def.functionContentVersion);
        ASSERT_EQ(2, def.functionDef.prototypes.size());
        ASSERT_EQ(2, def.functionDef.prototypes[0].paramTypes.size());
        ASSERT_TRUE(!def.functionDef.prototypes[0].paramTypes[0].isMulti);
        ASSERT_EQ("int64", def.functionDef.prototypes[0].paramTypes[0].type);
        ASSERT_TRUE(!def.functionDef.prototypes[0].paramTypes[1].isMulti);
        ASSERT_EQ("double", def.functionDef.prototypes[0].paramTypes[1].type);
        ASSERT_EQ(1, def.functionDef.prototypes[0].returnTypes.size());
        ASSERT_TRUE(!def.functionDef.prototypes[0].returnTypes[0].isMulti);
        ASSERT_EQ("string", def.functionDef.prototypes[0].returnTypes[0].type);
        ASSERT_EQ(1, def.functionDef.prototypes[0].accTypes.size());
        ASSERT_TRUE(!def.functionDef.prototypes[0].accTypes[0].isMulti);
        ASSERT_EQ("string", def.functionDef.prototypes[0].accTypes[0].type);

        ASSERT_EQ(2, def.functionDef.prototypes[1].paramTypes.size());
        ASSERT_TRUE(!def.functionDef.prototypes[1].paramTypes[0].isMulti);
        ASSERT_EQ("int64", def.functionDef.prototypes[1].paramTypes[0].type);
        ASSERT_TRUE(!def.functionDef.prototypes[1].paramTypes[1].isMulti);
        ASSERT_EQ("int64", def.functionDef.prototypes[1].paramTypes[1].type);
        ASSERT_EQ(1, def.functionDef.prototypes[1].returnTypes.size());
        ASSERT_TRUE(!def.functionDef.prototypes[1].returnTypes[0].isMulti);
        ASSERT_EQ("string", def.functionDef.prototypes[1].returnTypes[0].type);
        ASSERT_EQ(1, def.functionDef.prototypes[1].accTypes.size());
        ASSERT_TRUE(!def.functionDef.prototypes[1].accTypes[0].isMulti);
        ASSERT_EQ("string", def.functionDef.prototypes[1].accTypes[0].type);
    }
    {
        MockAggFuncCreator mockCreator;
        EXPECT_CALL(mockCreator, getAggFuncName())
            .WillOnce(Return(std::string("JoinSeriesDataAgg")));
        ASSERT_FALSE(mockCreator.initFunctionModel(
            "[string]", "[int64, int64]", "[string]|[string]", properties));
    }
    {
        MockAggFuncCreator mockCreator;
        EXPECT_CALL(mockCreator, getAggFuncName())
            .WillOnce(Return(std::string("JoinSeriesDataAgg")));
        ASSERT_FALSE(mockCreator.initFunctionModel(
            "[string]", "[int64, double]|[int64, int64]", "[string]", properties));
    }
    {
        MockAggFuncCreator mockCreator;
        EXPECT_CALL(mockCreator, getAggFuncName())
            .WillOnce(Return(std::string("JoinSeriesDataAgg")));
        ASSERT_FALSE(mockCreator.initFunctionModel(
            "[string]|[string]", "[int64, double]", "[string]", properties));
    }
}

TEST_F(AggFuncCreatorRTest, testPlugin) {
    navi::NaviResourceHelper naviHelper;
    naviHelper.setModules(
        {"./aios/sql/testdata/sql_agg_func/plugins/libsql_test_agg_plugin_lib.so"});
    auto aggFuncFactoryR = naviHelper.getOrCreateRes<AggFuncFactoryR>();
    ASSERT_TRUE(aggFuncFactoryR);
    auto func
        = aggFuncFactoryR->createAggFunction("sum2",
                                             {matchdoc::ValueTypeHelper<int32_t>::getValueType()},
                                             {"a"},
                                             {"b"},
                                             AggFuncMode::AGG_FUNC_MODE_LOCAL);
    ASSERT_TRUE(func != nullptr);
    delete func;
    auto func2 = aggFuncFactoryR->createAggFunction(
        "COUNT", {}, {}, {"a"}, AggFuncMode::AGG_FUNC_MODE_LOCAL);
    ASSERT_TRUE(func2);
    delete func2;
}

} // namespace sql
