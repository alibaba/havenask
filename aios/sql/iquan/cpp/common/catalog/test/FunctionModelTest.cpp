#include "iquan/common/catalog/FunctionModel.h"

#include <string>
#include <vector>

#include "autil/Log.h"
#include "iquan/common/catalog/FunctionCommonDef.h"
#include "iquan/common/catalog/FunctionDef.h"
#include "unittest/unittest.h"

using namespace testing;

namespace iquan {

class FunctionModelTest : public TESTBASE {
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(iquan, FunctionModelTest);

TEST_F(FunctionModelTest, testConstructor) {
    FunctionModel functionModel;
    ASSERT_FALSE(functionModel.isValid());

    ASSERT_EQ("", functionModel.functionName);
    ASSERT_EQ("", functionModel.functionType);
    ASSERT_EQ(1, functionModel.functionVersion);
    ASSERT_EQ(0, functionModel.isDeterministic);
    ASSERT_EQ("", functionModel.functionContentVersion);

    const FunctionDef &functionDef = functionModel.functionDef;
    ASSERT_EQ(0, functionDef.prototypes.size());
}

TEST_F(FunctionModelTest, testIsValid) {
    FunctionModel functionModel;
    ASSERT_FALSE(functionModel.isValid());

    functionModel.functionName = "test";
    ASSERT_TRUE(!functionModel.isValid());
    ASSERT_EQ("test", functionModel.functionName);

    functionModel.functionType = "testType";
    ASSERT_TRUE(!functionModel.isValid());
    ASSERT_EQ("testType", functionModel.functionType);

    functionModel.functionVersion = 100;
    ASSERT_TRUE(!functionModel.isValid());
    ASSERT_EQ(100, functionModel.functionVersion);

    functionModel.isDeterministic = 1;
    ASSERT_TRUE(!functionModel.isValid());
    ASSERT_EQ(1, functionModel.isDeterministic);

    functionModel.functionContentVersion = "test_content";
    ASSERT_TRUE(!functionModel.isValid());
    ASSERT_EQ("test_content", functionModel.functionContentVersion);

    FunctionDef functionDef;
    {
        ParamTypeDef paramTypeDef;
        paramTypeDef.isMulti = true;
        paramTypeDef.type = "STRING";

        PrototypeDef prototypeDef;
        prototypeDef.returnTypes.emplace_back(paramTypeDef);
        prototypeDef.paramTypes.emplace_back(paramTypeDef);
        prototypeDef.accTypes.emplace_back(paramTypeDef);
        ASSERT_TRUE(prototypeDef.isValid());

        functionDef.prototypes.emplace_back(prototypeDef);
    }

    functionModel.functionDef = functionDef;
    ASSERT_TRUE(functionModel.isValid());

    functionModel.isDeterministic = 0;
    ASSERT_TRUE(functionModel.isValid());

    functionModel.isDeterministic = 100;
    ASSERT_FALSE(functionModel.isValid());
}

} // namespace iquan
