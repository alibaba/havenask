#include "sql/ops/tvf/TvfFuncCreatorR.h"

#include <algorithm>
#include <iosfwd>
#include <map>
#include <string>
#include <vector>

#include "iquan/common/catalog/FunctionCommonDef.h"
#include "iquan/common/catalog/TvfFunctionDef.h"
#include "iquan/common/catalog/TvfFunctionModel.h"
#include "navi/engine/Resource.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/ops/tvf/SqlTvfProfileInfo.h"
#include "sql/ops/tvf/TvfFunc.h"
#include "sql/ops/tvf/test/MockTvfFunc.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace sql {

class TvfFuncCreatorRTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void TvfFuncCreatorRTest::setUp() {}

void TvfFuncCreatorRTest::tearDown() {}

const string tvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "rankTvf",
  "function_type": "TVF",
  "is_deterministic": 1,
  "function_content_version": "json_default_0.1",
  "function_content": {
    "properties": {
      "enable_shuffle": false
    },
    "prototypes": [
      {
        "params": {
          "scalars": [
             {
               "type":"string"
             },
             {
               "type":"string"
             },
             {
               "type":"string"
             }
         ],
          "tables": [
            {
              "auto_infer": true
            }
          ]
        },
        "returns": {
          "new_fields": [
          ],
          "tables": [
            {
              "auto_infer": true
            }
          ]
        }
      }
    ]
  }
}
)tvf_json";

const string errorTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "rankTvf",
  "function_type": "TVF",
  "is_deterministic": 1,
  "function_content_version": "json_default_0.1",
  "function_content": {
    "properties": {
      "enable_shuffle": false
    },
    "prototypes": [
      {
        "params": {
          "scalars": [
             {
               "type":"int"
             }
         ],
          "tables": [
            {
              "auto_infer": true
            }
          ]
        },
        "returns": {
          "new_fields": [
          ],
          "tables": [
            {
              "auto_infer": true
            }
          ]
        }
      }
    ]
  }
}
)tvf_json";

class NormalTvfFuncCreator : public TvfFuncCreatorR {
public:
    NormalTvfFuncCreator()
        : TvfFuncCreatorR(tvfDef, SqlTvfProfileInfo("abc", "def")) {}

public:
    std::string getResourceName() const override {
        return RESOURCE_ID;
    }

public:
    static const std::string RESOURCE_ID;

private:
    TvfFunc *createFunction(const SqlTvfProfileInfo &info) override {
        return new MockTvfFunc();
    }
};
const std::string NormalTvfFuncCreator::RESOURCE_ID = "tvf_test.normal_tvf";
REGISTER_RESOURCE(NormalTvfFuncCreator);

TEST_F(TvfFuncCreatorRTest, testSimple) {
    {
        navi::NaviResourceHelper naviRHelper;
        auto creator = naviRHelper.getOrCreateRes<NormalTvfFuncCreator>();
        ASSERT_TRUE(creator);
        EXPECT_EQ(NormalTvfFuncCreator::RESOURCE_ID, creator->getName());
        EXPECT_EQ(1u, creator->_sqlTvfProfileInfos.size());
        EXPECT_TRUE(creator->_sqlTvfProfileInfos.end() != creator->_sqlTvfProfileInfos.find("abc"));
        EXPECT_FALSE(creator->TvfFuncCreatorR::createFunction(std::string("xyz")));
        auto func = creator->TvfFuncCreatorR::createFunction(std::string("abc"));
        EXPECT_TRUE(dynamic_cast<MockTvfFunc *>(func));
        EXPECT_TRUE(func);
        EXPECT_EQ("abc", func->getName());
        delete func;
    }
    {
        std::string configStr = R"tvf_json(
        {
            "tvf_profiles" : [
                {
                    "tvf_name" : "test_tvf_1",
                    "func_name" : "test_func_1"
                },
                {
                    "tvf_name" : "test_tvf_2",
                    "func_name" : "test_func_2"
                }
            ]
        }
        )tvf_json";
        navi::NaviResourceHelper naviRHelper;
        naviRHelper.logLevel("info");
        ASSERT_TRUE(naviRHelper.config(NormalTvfFuncCreator::RESOURCE_ID, configStr));
        auto creator = naviRHelper.getOrCreateRes<NormalTvfFuncCreator>();
        ASSERT_TRUE(creator);
        EXPECT_EQ(NormalTvfFuncCreator::RESOURCE_ID, creator->getName());
        EXPECT_EQ(3u, creator->_sqlTvfProfileInfos.size());
        EXPECT_TRUE(creator->_sqlTvfProfileInfos.end() != creator->_sqlTvfProfileInfos.find("abc"));
        EXPECT_TRUE(creator->_sqlTvfProfileInfos.end()
                    != creator->_sqlTvfProfileInfos.find("test_tvf_1"));
        EXPECT_TRUE(creator->_sqlTvfProfileInfos.end()
                    != creator->_sqlTvfProfileInfos.find("test_tvf_2"));
    }
}

TEST_F(TvfFuncCreatorRTest, testGenerateTvfModel) {
    navi::NaviLoggerProvider provider;
    iquan::TvfModel tvfModel;
    SqlTvfProfileInfo info({"abc", "abc"});
    {
        navi::NaviResourceHelper naviRHelper;
        auto creator = naviRHelper.getOrCreateRes<NormalTvfFuncCreator>();
        ASSERT_TRUE(creator);
        creator->_tvfDef = "xxx";
        ASSERT_FALSE(creator->generateTvfModel(tvfModel, info));
        ASSERT_EQ("register tvf models failed [xxx].", provider.getErrorMessage());
    }
    {
        navi::NaviResourceHelper naviRHelper;
        auto creator = naviRHelper.getOrCreateRes<NormalTvfFuncCreator>();
        ASSERT_TRUE(creator);
        ASSERT_TRUE(creator->generateTvfModel(tvfModel, info));
        ASSERT_EQ("abc", tvfModel.functionName);
        ASSERT_EQ("TVF", tvfModel.functionType);
    }
}

TEST_F(TvfFuncCreatorRTest, testCheckTvfModel) {
    {
        iquan::TvfModel tvfModel;
        ASSERT_FALSE(TvfFuncCreatorR::checkTvfModel(tvfModel));
    }
    {
        iquan::TvfModel tvfModel;
        iquan::TvfDef tvfDef;
        iquan::ParamTypeDef paramDef;
        paramDef.type = "int";
        tvfDef.params.scalars.push_back(paramDef);
        tvfModel.functionContent.tvfs.push_back(tvfDef);
        ASSERT_FALSE(TvfFuncCreatorR::checkTvfModel(tvfModel));
    }
    {
        iquan::TvfModel tvfModel;
        tvfModel.functionContent.tvfs.push_back(iquan::TvfDef());
        ASSERT_TRUE(TvfFuncCreatorR::checkTvfModel(tvfModel));
    }
    {
        iquan::TvfModel tvfModel;
        iquan::TvfDef tvfDef;
        iquan::ParamTypeDef paramDef;
        paramDef.type = "string";
        tvfDef.params.scalars.push_back(paramDef);
        tvfModel.functionContent.tvfs.push_back(tvfDef);
        ASSERT_TRUE(TvfFuncCreatorR::checkTvfModel(tvfModel));
    }
}

TEST_F(TvfFuncCreatorRTest, test_regTvfModels_failed) {
    {
        navi::NaviResourceHelper naviRHelper;
        auto creator = naviRHelper.getOrCreateRes<NormalTvfFuncCreator>();
        ASSERT_TRUE(creator);
        {
            iquan::TvfModels tvfModels;
            navi::NaviLoggerProvider provider;
            creator->_tvfDef = "xxx";
            ASSERT_FALSE(creator->regTvfModels(tvfModels));
            ASSERT_EQ(0u, tvfModels.functions.size());
            ASSERT_EQ("register tvf models failed [xxx].", provider.getErrorMessage());
        }
        {
            iquan::TvfModels tvfModels;
            navi::NaviLoggerProvider provider;
            creator->_tvfDef = errorTvfDef;
            ASSERT_FALSE(creator->regTvfModels(tvfModels));
            ASSERT_EQ(0u, tvfModels.functions.size());
            ASSERT_EQ("tvf [abc] func prototypes only support string type. now is [int]",
                      provider.getErrorMessage());
        }
        {
            creator->_tvfDef = tvfDef;
            creator->_sqlTvfProfileInfos.clear();
            creator->_sqlTvfProfileInfos["test_1"] = SqlTvfProfileInfo("x1", "y1");
            creator->_sqlTvfProfileInfos["test_2"] = SqlTvfProfileInfo("x2", "y2");

            iquan::TvfModels tvfModels;
            ASSERT_TRUE(creator->regTvfModels(tvfModels));
            ASSERT_EQ(2u, tvfModels.functions.size());
        }
    }
}

} // namespace sql
