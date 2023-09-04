#include "iquan/common/catalog/TvfFunctionDef.h"

#include <iosfwd>
#include <map>
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

using namespace std;
using namespace testing;

namespace iquan {

class TvfFunctionDefTest : public TESTBASE {
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(iquan, TvfFunctionDefTest);

TEST_F(TvfFunctionDefTest, testTvfFieldDef) {
    // simple type for TvfFieldDef
    {
        TvfFieldDef field;
        field.fieldName = "i1";
        field.fieldType = {false, "int32"};

        std::string actualFieldStr;
        Status status = Utils::toJson(field, actualFieldStr, false);
        ASSERT_TRUE(status.ok()) << status.errorMessage();

        std::string expectedFieldStr = R"json({
"field_name":
  "i1",
"field_type":
  {
  "type":
    "int32"
  }
})json";
        ASSERT_NO_FATAL_FAILURE(
            autil::legacy::JsonTestUtil::checkJsonStringEqual(expectedFieldStr, actualFieldStr));
    }

    // complex type for tvf field def
    {
        TvfFieldDef field;
        field.fieldName = "i1";
        field.fieldType = {true, "int32"};

        std::string actualFieldStr;
        Status status = Utils::toJson(field, actualFieldStr, false);
        ASSERT_TRUE(status.ok()) << status.errorMessage();

        std::string expectedFieldStr = R"json({
"field_name":
  "i1",
"field_type":
  {
  "type":
    "array",
  "value_type":
    {
    "type":
      "int32"
    }
  }
})json";
        ASSERT_NO_FATAL_FAILURE(
            autil::legacy::JsonTestUtil::checkJsonStringEqual(expectedFieldStr, actualFieldStr));
    }
}

TEST_F(TvfFunctionDefTest, testTvfInputTableDef) {
    // simple
    {
        std::string expectedTvfInputTableDefStr = R"json({
"auto_infer":
  true,
"check_fields":
  [
    {
    "field_name":
      "i1",
    "field_type":
      {
      "type":
        "int32"
      }
    },
    {
    "field_name":
      "i2",
    "field_type":
      {
      "type":
        "int64"
      }
    }
  ],
"input_fields":
  [
  ]
})json";

        TvfInputTableDef inputTableDef;

        inputTableDef.autoInfer = true;

        TvfFieldDef checkType1;
        checkType1.fieldName = "i1";
        checkType1.fieldType = {false, "int32"};
        inputTableDef.checkFields.emplace_back(checkType1);

        TvfFieldDef checkType2;
        checkType2.fieldName = "i2";
        checkType2.fieldType = {false, "int64"};
        inputTableDef.checkFields.emplace_back(checkType2);

        std::string tvfInputTableDefStr;
        Status status = Utils::toJson(inputTableDef, tvfInputTableDefStr, false);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_NO_FATAL_FAILURE(autil::legacy::JsonTestUtil::checkJsonStringEqual(
            expectedTvfInputTableDefStr, tvfInputTableDefStr));
    }

    // multi array as check field
    {
        std::string expectedTvfInputTableDefStr = R"json({
"auto_infer":
  true,
"check_fields":
  [
    {
    "field_name":
      "i1",
    "field_type":
      {
      "type":
        "array",
      "value_type":
        {
        "type":
          "int32"
        }
      }
    },
    {
    "field_name":
      "i2",
    "field_type":
      {
      "type":
        "int64"
      }
    }
  ],
"input_fields":
  [
  ]
})json";

        TvfInputTableDef inputTableDef;

        inputTableDef.autoInfer = true;

        TvfFieldDef checkType1;
        checkType1.fieldName = "i1";
        checkType1.fieldType = {true, "int32"};
        inputTableDef.checkFields.emplace_back(checkType1);

        TvfFieldDef checkType2;
        checkType2.fieldName = "i2";
        checkType2.fieldType = {false, "int64"};
        inputTableDef.checkFields.emplace_back(checkType2);

        std::string tvfInputTableDefStr;
        Status status = Utils::toJson(inputTableDef, tvfInputTableDefStr, false);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_NO_FATAL_FAILURE(autil::legacy::JsonTestUtil::checkJsonStringEqual(
            expectedTvfInputTableDefStr, tvfInputTableDefStr));
    }

    // we have both input_fields and check_fields
    {
        std::string expectedTvfInputTableDefStr = R"json({
"auto_infer":
  false,
"check_fields":
  [
    {
    "field_name":
      "i4",
    "field_type":
      {
      "type":
        "array",
      "value_type":
        {
        "type":
          "int32"
        }
      }
    },
    {
    "field_name":
      "i5",
    "field_type":
      {
      "type":
        "int64"
      }
    }
  ],
"input_fields":
  [
    {
    "field_name":
      "float1",
    "field_type":
      {
      "type":
        "array",
      "value_type":
        {
        "type":
          "float"
        }
      }
    },
    {
    "field_name":
      "double2",
    "field_type":
      {
      "type":
        "array",
      "value_type":
        {
        "type":
          "double"
        }
      }
    },
    {
    "field_name":
      "long3",
    "field_type":
      {
      "type":
        "array",
      "value_type":
        {
        "type":
          "int64"
        }
      }
    }
  ]
})json";

        TvfInputTableDef inputTableDef;

        // auto info
        inputTableDef.autoInfer = false;

        // input field
        TvfFieldDef inputField1;
        inputField1.fieldName = "float1";
        inputField1.fieldType = {true, "float"};
        inputTableDef.inputFields.emplace_back(inputField1);

        TvfFieldDef inputField2;
        inputField2.fieldName = "double2";
        inputField2.fieldType = {true, "double"};
        inputTableDef.inputFields.emplace_back(inputField2);

        TvfFieldDef inputField3;
        inputField3.fieldName = "long3";
        inputField3.fieldType = {true, "int64"};
        inputTableDef.inputFields.emplace_back(inputField3);

        // check field
        TvfFieldDef checkType1;
        checkType1.fieldName = "i4";
        checkType1.fieldType = {true, "int32"};
        inputTableDef.checkFields.emplace_back(checkType1);

        TvfFieldDef checkType2;
        checkType2.fieldName = "i5";
        checkType2.fieldType = {false, "int64"};
        inputTableDef.checkFields.emplace_back(checkType2);

        std::string tvfInputTableDefStr;
        Status status = Utils::toJson(inputTableDef, tvfInputTableDefStr, false);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_NO_FATAL_FAILURE(autil::legacy::JsonTestUtil::checkJsonStringEqual(
            expectedTvfInputTableDefStr, tvfInputTableDefStr));
    }
}

TEST_F(TvfFunctionDefTest, testTvfParamsDef) {
    {
        // 1. construct the input table
        TvfInputTableDef inputTableDef;

        // auto info
        inputTableDef.autoInfer = false;

        // input field
        TvfFieldDef inputField1;
        inputField1.fieldName = "float1";
        inputField1.fieldType = {true, "float"};
        inputTableDef.inputFields.emplace_back(inputField1);

        TvfFieldDef inputField2;
        inputField2.fieldName = "double2";
        inputField2.fieldType = {true, "double"};
        inputTableDef.inputFields.emplace_back(inputField2);

        TvfFieldDef inputField3;
        inputField3.fieldName = "long3";
        inputField3.fieldType = {true, "int64"};
        inputTableDef.inputFields.emplace_back(inputField3);

        // check field
        TvfFieldDef checkType1;
        checkType1.fieldName = "i4";
        checkType1.fieldType = {true, "int32"};
        inputTableDef.checkFields.emplace_back(checkType1);

        TvfFieldDef checkType2;
        checkType2.fieldName = "i5";
        checkType2.fieldType = {false, "int64"};
        inputTableDef.checkFields.emplace_back(checkType2);

        std::string tvfInputTableDefStr;
        Status status = Utils::toJson(inputTableDef, tvfInputTableDefStr, false);
        ASSERT_TRUE(status.ok()) << status.errorMessage();

        // 2. construct the scalar params
        std::vector<ParamTypeDef> scalars;
        scalars.push_back({false, "string"});
        scalars.push_back({true, "string"});
        scalars.push_back({false, "int32"});

        // 3. construct TvfParamsDef
        TvfParamsDef tvfParams;
        tvfParams.scalars = scalars;
        tvfParams.tables.emplace_back(inputTableDef);

        // 4. test
        std::string expectedStr = R"json({
"scalars":
  [
    {
    "type":
      "string"
    },
    {
    "type":
      "array",
    "value_type":
      {
      "type":
        "string"
      }
    },
    {
    "type":
      "int32"
    }
  ],
"tables":
  [
    {
    "auto_infer":
      false,
    "check_fields":
      [
        {
        "field_name":
          "i4",
        "field_type":
          {
          "type":
            "array",
          "value_type":
            {
            "type":
              "int32"
            }
          }
        },
        {
        "field_name":
          "i5",
        "field_type":
          {
          "type":
            "int64"
          }
        }
      ],
    "input_fields":
      [
        {
        "field_name":
          "float1",
        "field_type":
          {
          "type":
            "array",
          "value_type":
            {
            "type":
              "float"
            }
          }
        },
        {
        "field_name":
          "double2",
        "field_type":
          {
          "type":
            "array",
          "value_type":
            {
            "type":
              "double"
            }
          }
        },
        {
        "field_name":
          "long3",
        "field_type":
          {
          "type":
            "array",
          "value_type":
            {
            "type":
              "int64"
            }
          }
        }
      ]
    }
  ]
})json";
        std::string actualStr;

        status = Utils::toJson(tvfParams, actualStr, false);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_NO_FATAL_FAILURE(
            autil::legacy::JsonTestUtil::checkJsonStringEqual(expectedStr, actualStr));
    }
}

TEST_F(TvfFunctionDefTest, testTvfOutputTableDef) {
    {
        TvfOutputTableDef ouputTable;
        ouputTable.autoInfer = true;

        std::string expectedStr = R"json({
"auto_infer":
  true,
"field_names":
  [
  ]
})json";
        std::string actualStr;
        Status status = Utils::toJson(ouputTable, actualStr, false);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_NO_FATAL_FAILURE(
            autil::legacy::JsonTestUtil::checkJsonStringEqual(expectedStr, actualStr));
    }

    {
        TvfOutputTableDef ouputTable;
        ouputTable.autoInfer = true;
        ouputTable.fieldNames.push_back("i1");
        ouputTable.fieldNames.push_back("i2");
        ouputTable.fieldNames.push_back("i2");

        std::string expectedStr = R"json({
"auto_infer":
  true,
"field_names":
  [
    "i1",
    "i2",
    "i2"
  ]
})json";
        std::string actualStr;
        Status status = Utils::toJson(ouputTable, actualStr, false);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_NO_FATAL_FAILURE(
            autil::legacy::JsonTestUtil::checkJsonStringEqual(expectedStr, actualStr));
    }
}

TEST_F(TvfFunctionDefTest, testTvfReturnsDef) {
    {
        TvfReturnsDef returns;

        // 1. construct output table
        TvfOutputTableDef ouputTable;
        ouputTable.autoInfer = true;
        ouputTable.fieldNames.push_back("i1");
        ouputTable.fieldNames.push_back("i2");
        ouputTable.fieldNames.push_back("i2");
        returns.outputTables.emplace_back(ouputTable);

        // 2. construct newFields
        returns.newFields.push_back({"new1", {true, "int32"}});
        returns.newFields.push_back({"new2", {true, "int64"}});
        returns.newFields.push_back({"new3", {false, "float"}});

        std::string expectedStr = R"json({
"new_fields":
  [
    {
    "field_name":
      "new1",
    "field_type":
      {
      "type":
        "array",
      "value_type":
        {
        "type":
          "int32"
        }
      }
    },
    {
    "field_name":
      "new2",
    "field_type":
      {
      "type":
        "array",
      "value_type":
        {
        "type":
          "int64"
        }
      }
    },
    {
    "field_name":
      "new3",
    "field_type":
      {
      "type":
        "float"
      }
    }
  ],
"tables":
  [
    {
    "auto_infer":
      true,
    "field_names":
      [
        "i1",
        "i2",
        "i2"
      ]
    }
  ]
})json";
        std::string actualStr;
        Status status = Utils::toJson(returns, actualStr, false);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_NO_FATAL_FAILURE(
            autil::legacy::JsonTestUtil::checkJsonStringEqual(expectedStr, actualStr));
    }
}

TEST_F(TvfFunctionDefTest, testTvfFunctionDef) {
    TvfDef tvfDef;

    // 1. construct the input table
    TvfInputTableDef inputTableDef;

    // auto info
    inputTableDef.autoInfer = false;

    // input field
    TvfFieldDef inputField1;
    inputField1.fieldName = "float1";
    inputField1.fieldType = {true, "float"};
    inputTableDef.inputFields.emplace_back(inputField1);

    TvfFieldDef inputField2;
    inputField2.fieldName = "double2";
    inputField2.fieldType = {true, "double"};
    inputTableDef.inputFields.emplace_back(inputField2);

    TvfFieldDef inputField3;
    inputField3.fieldName = "long3";
    inputField3.fieldType = {true, "int64"};
    inputTableDef.inputFields.emplace_back(inputField3);

    // check field
    TvfFieldDef checkType1;
    checkType1.fieldName = "i4";
    checkType1.fieldType = {true, "int32"};
    inputTableDef.checkFields.emplace_back(checkType1);

    TvfFieldDef checkType2;
    checkType2.fieldName = "i5";
    checkType2.fieldType = {false, "int64"};
    inputTableDef.checkFields.emplace_back(checkType2);

    std::string tvfInputTableDefStr;
    Status status = Utils::toJson(inputTableDef, tvfInputTableDefStr, false);
    ASSERT_TRUE(status.ok()) << status.errorMessage();

    // 2. construct the scalar params
    std::vector<ParamTypeDef> scalars;
    scalars.push_back({false, "string"});
    scalars.push_back({true, "string"});
    scalars.push_back({false, "int32"});

    // 3. construct TvfParamsDef
    TvfParamsDef tvfParams;
    tvfParams.scalars = scalars;
    tvfParams.tables.emplace_back(inputTableDef);

    // 4. return tables
    TvfReturnsDef returns;
    TvfOutputTableDef ouputTable;

    ouputTable.autoInfer = true;
    ouputTable.fieldNames.push_back("i1");
    ouputTable.fieldNames.push_back("i2");
    returns.outputTables.emplace_back(ouputTable);

    returns.newFields.push_back({"new_int64_1", {true, "int64"}});
    returns.newFields.push_back({"new_int32_1", {false, "int32"}});

    // 5. connect to the TvfFunctionDef
    tvfDef.params = tvfParams;
    tvfDef.returns = returns;

    TvfFunctionDef tvfFunctionDef;
    // 6. set distribution
    tvfFunctionDef.distribution.partitionCnt = 100;

    // 7. set kvpair
    tvfFunctionDef.properties["key"] = autil::legacy::Any(std::string("value"));
    tvfFunctionDef.properties["key2"] = 100;

    // 6. check
    tvfFunctionDef.tvfs.emplace_back(tvfDef);
    const std::string expectedStr = R"json({
"distribution":
  {
  "partition_cnt":
    100
  },
"properties":
  {
  "key":
    "value",
  "key2":
    100
  },
"prototypes":
  [
    {
    "params":
      {
      "scalars":
        [
          {
          "type":
            "string"
          },
          {
          "type":
            "array",
          "value_type":
            {
            "type":
              "string"
            }
          },
          {
          "type":
            "int32"
          }
        ],
      "tables":
        [
          {
          "auto_infer":
            false,
          "check_fields":
            [
              {
              "field_name":
                "i4",
              "field_type":
                {
                "type":
                  "array",
                "value_type":
                  {
                  "type":
                    "int32"
                  }
                }
              },
              {
              "field_name":
                "i5",
              "field_type":
                {
                "type":
                  "int64"
                }
              }
            ],
          "input_fields":
            [
              {
              "field_name":
                "float1",
              "field_type":
                {
                "type":
                  "array",
                "value_type":
                  {
                  "type":
                    "float"
                  }
                }
              },
              {
              "field_name":
                "double2",
              "field_type":
                {
                "type":
                  "array",
                "value_type":
                  {
                  "type":
                    "double"
                  }
                }
              },
              {
              "field_name":
                "long3",
              "field_type":
                {
                "type":
                  "array",
                "value_type":
                  {
                  "type":
                    "int64"
                  }
                }
              }
            ]
          }
        ]
      },
    "returns":
      {
      "new_fields":
        [
          {
          "field_name":
            "new_int64_1",
          "field_type":
            {
            "type":
              "array",
            "value_type":
              {
              "type":
                "int64"
              }
            }
          },
          {
          "field_name":
            "new_int32_1",
          "field_type":
            {
            "type":
              "int32"
            }
          }
        ],
      "tables":
        [
          {
          "auto_infer":
            true,
          "field_names":
            [
              "i1",
              "i2"
            ]
          }
        ]
      }
    }
  ]
})json";
    std::string actualStr;
    status = Utils::toJson(tvfFunctionDef, actualStr, false);
    ASSERT_TRUE(status.ok()) << status.errorMessage();
    ASSERT_NO_FATAL_FAILURE(
        autil::legacy::JsonTestUtil::checkJsonStringEqual(expectedStr, actualStr));
}

TEST_F(TvfFunctionDefTest, testTvfDistributionDef) {
    TvfDistributionDef distribution;
    distribution.partitionCnt = 1000;

    std::string expectedStr = R"json({
"partition_cnt":
  1000
})json";
    std::string actualStr;

    Status status = Utils::toJson(distribution, actualStr, false);
    ASSERT_TRUE(status.ok()) << status.errorMessage();
    ASSERT_NO_FATAL_FAILURE(
        autil::legacy::JsonTestUtil::checkJsonStringEqual(expectedStr, actualStr));
}

} // namespace iquan
