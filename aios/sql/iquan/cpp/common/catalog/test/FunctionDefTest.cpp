#include "iquan/common/catalog/FunctionDef.h"

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/test/JsonTestUtil.h"
#include "iquan/common/IquanException.h"
#include "iquan/common/Status.h"
#include "iquan/common/Utils.h"
#include "iquan/common/catalog/FunctionCommonDef.h"
#include "unittest/unittest.h"

using namespace testing;

namespace iquan {

class FunctionDefTest : public TESTBASE {
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(iquan, FunctionDefTest);

TEST_F(FunctionDefTest, testParamTypeDef) {
    ParamTypeDef paramTypeDef;
    ASSERT_FALSE(paramTypeDef.isMulti);
    ASSERT_EQ("", paramTypeDef.type);
    ASSERT_FALSE(paramTypeDef.isValid());

    paramTypeDef.isMulti = true;
    ASSERT_TRUE(paramTypeDef.isMulti);
    paramTypeDef.type = "test";
    ASSERT_EQ("test", paramTypeDef.type);
    ASSERT_TRUE(paramTypeDef.isValid());
}

TEST_F(FunctionDefTest, testParamTypeDefJsonize) {
    {
        ParamTypeDef paramTypeDef;
        paramTypeDef.isMulti = true;
        paramTypeDef.type = "test";

        std::string actualStr = "";
        std::string expectedStr = R"json({
"type":
  "test"
})json";
        Status status = Utils::toJson(paramTypeDef, actualStr, false);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_NO_FATAL_FAILURE(
            autil::legacy::JsonTestUtil::checkJsonStringEqual(expectedStr, actualStr));

        ParamTypeDef paramTypeDef2;
        status = Utils::fromJson(paramTypeDef2, actualStr);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
    }

    {
        ParamTypeDef paramTypeDef;
        paramTypeDef.isMulti = true;
        paramTypeDef.type = "test";
        paramTypeDef.extendInfos["key"] = "value";
        paramTypeDef.keyType["key1"] = autil::legacy::Any(std::string("value1"));
        paramTypeDef.valueType["key2"] = autil::legacy::Any(std::string("value2"));

        std::string actualStr = "";
        std::string expectedStr = R"json({
"extend_infos":
  {
  "key":
    "value"
  },
"key_type":
  {
  "key1":
    "value1"
  },
"type":
  "test",
"value_type":
  {
  "key2":
    "value2"
  }
})json";
        Status status = Utils::toJson(paramTypeDef, actualStr, false);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_NO_FATAL_FAILURE(
            autil::legacy::JsonTestUtil::checkJsonStringEqual(expectedStr, actualStr));

        ParamTypeDef paramTypeDef2;
        status = Utils::fromJson(paramTypeDef2, actualStr);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
    }
}

TEST_F(FunctionDefTest, testPrototypeDef) {
    PrototypeDef prototypeDef;
    ASSERT_EQ(0, prototypeDef.returnTypes.size());
    ASSERT_EQ(0, prototypeDef.paramTypes.size());
    ASSERT_FALSE(prototypeDef.isValid());

    ParamTypeDef paramTypeDef;
    paramTypeDef.isMulti = true;
    paramTypeDef.type = "STRING";

    prototypeDef.returnTypes.emplace_back(paramTypeDef);
    ASSERT_FALSE(prototypeDef.isValid());

    prototypeDef.paramTypes.emplace_back(paramTypeDef);
    ASSERT_TRUE(prototypeDef.isValid());

    prototypeDef.returnTypes.clear();
    ASSERT_FALSE(prototypeDef.isValid());

    prototypeDef.returnTypes.emplace_back(paramTypeDef);
    ASSERT_TRUE(prototypeDef.isValid());

    prototypeDef.accTypes.emplace_back(paramTypeDef);
    ASSERT_TRUE(prototypeDef.isValid());

    std::string actualStr = "";
    std::string expectedStr = R"json({
"acc_types":
  [
    {
    "type":
      "STRING"
    }
  ],
"params":
  [
    {
    "type":
      "STRING"
    }
  ],
"returns":
  [
    {
    "type":
      "STRING"
    }
  ],
"variable_args":
  false
})json";
    Status status = Utils::toJson(prototypeDef, actualStr, false);
    ASSERT_TRUE(status.ok()) << status.errorMessage();
    ASSERT_NO_FATAL_FAILURE(
        autil::legacy::JsonTestUtil::checkJsonStringEqual(expectedStr, actualStr));

    PrototypeDef prototypeDef2;
    status = Utils::fromJson(prototypeDef2, actualStr);
    ASSERT_TRUE(status.ok()) << status.errorMessage();
}

TEST_F(FunctionDefTest, testFunctionDef) {
    FunctionDef functionDef;
    ASSERT_EQ(0, functionDef.prototypes.size());
    ASSERT_FALSE(functionDef.isValid());

    ParamTypeDef paramTypeDef;
    paramTypeDef.isMulti = true;
    paramTypeDef.type = "STRING";

    PrototypeDef prototypeDef;
    ASSERT_FALSE(prototypeDef.isValid());

    {
        prototypeDef.returnTypes.emplace_back(paramTypeDef);
        ASSERT_FALSE(prototypeDef.isValid());

        prototypeDef.paramTypes.emplace_back(paramTypeDef);
        ASSERT_TRUE(prototypeDef.isValid());

        prototypeDef.returnTypes.clear();
        ASSERT_FALSE(prototypeDef.isValid());

        prototypeDef.returnTypes.emplace_back(paramTypeDef);
        ASSERT_TRUE(prototypeDef.isValid());
    }

    functionDef.prototypes.emplace_back(prototypeDef);
    ASSERT_TRUE(functionDef.isValid());

    {
        std::string actualStr = "";
        std::string expectedStr = R"json({
"properties":
  {
  },
"prototypes":
  [
    {
    "params":
      [
        {
        "type":
          "STRING"
        }
      ],
    "returns":
      [
        {
        "type":
          "STRING"
        }
      ],
    "variable_args":
      false
    }
  ]
})json";
        Status status = Utils::toJson(functionDef, actualStr, false);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_NO_FATAL_FAILURE(
            autil::legacy::JsonTestUtil::checkJsonStringEqual(expectedStr, actualStr));

        FunctionDef functionDef2;
        status = Utils::fromJson(functionDef2, actualStr);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
    }

    {
        prototypeDef.accTypes.emplace_back(paramTypeDef);
        functionDef.prototypes.emplace_back(prototypeDef);
        ASSERT_TRUE(functionDef.isValid());

        std::string actualStr = "";
        std::string expectedStr = R"json({
"properties":
  {
  },
"prototypes":
  [
    {
    "params":
      [
        {
        "type":
          "STRING"
        }
      ],
    "returns":
      [
        {
        "type":
          "STRING"
        }
      ],
    "variable_args":
      false
    },
    {
    "acc_types":
      [
        {
        "type":
          "STRING"
        }
      ],
    "params":
      [
        {
        "type":
          "STRING"
        }
      ],
    "returns":
      [
        {
        "type":
          "STRING"
        }
      ],
    "variable_args":
      false
    }
  ]
})json";
        Status status = Utils::toJson(functionDef, actualStr, false);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_NO_FATAL_FAILURE(
            autil::legacy::JsonTestUtil::checkJsonStringEqual(expectedStr, actualStr));

        FunctionDef functionDef2;
        status = Utils::fromJson(functionDef2, actualStr);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
    }

    {
        ASSERT_EQ(2, functionDef.prototypes.size());
        functionDef.prototypes[0].variableArgs = true;

        std::string actualStr = "";
        std::string expectedStr = R"json({
"properties":
  {
  },
"prototypes":
  [
    {
    "params":
      [
        {
        "type":
          "STRING"
        }
      ],
    "returns":
      [
        {
        "type":
          "STRING"
        }
      ],
    "variable_args":
      true
    },
    {
    "acc_types":
      [
        {
        "type":
          "STRING"
        }
      ],
    "params":
      [
        {
        "type":
          "STRING"
        }
      ],
    "returns":
      [
        {
        "type":
          "STRING"
        }
      ],
    "variable_args":
      false
    }
  ]
})json";
        Status status = Utils::toJson(functionDef, actualStr, false);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_NO_FATAL_FAILURE(
            autil::legacy::JsonTestUtil::checkJsonStringEqual(expectedStr, actualStr));

        FunctionDef functionDef2;
        status = Utils::fromJson(functionDef2, actualStr);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
    }
}

} // namespace iquan
