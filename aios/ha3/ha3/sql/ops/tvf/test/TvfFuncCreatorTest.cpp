#include <ha3/sql/ops/test/OpTestBase.h>
#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/sql/ops/tvf/TvfFuncCreator.h"

using namespace std;
using namespace testing;

USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(sql);

class TvfFuncCreatorTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(sql, TvfFuncCreatorTest);

void TvfFuncCreatorTest::setUp() {
}

void TvfFuncCreatorTest::tearDown() {
}

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

class NormalTvfFuncCreator : public TvfFuncCreator {
public:
    NormalTvfFuncCreator(const std::string &tvfDef,
                         HA3_NS(config)::SqlTvfProfileInfo info =
                         HA3_NS(config)::SqlTvfProfileInfo())
        : TvfFuncCreator(tvfDef, info)
    {}
private:
    TvfFunc *createFunction(const HA3_NS(config)::SqlTvfProfileInfo &info) override {
        return nullptr;
    }
};

TEST_F(TvfFuncCreatorTest, testConstruct) {
    {
        NormalTvfFuncCreator creator("");
        ASSERT_EQ("", creator._tvfDef);
        ASSERT_EQ("", creator._name);
        ASSERT_EQ(0, creator._sqlTvfProfileInfos.size());
    }
    {
        NormalTvfFuncCreator creator("abc", {"a", ""});
        ASSERT_EQ("abc", creator._tvfDef);
        ASSERT_EQ("", creator._name);
        ASSERT_EQ(0, creator._sqlTvfProfileInfos.size());
    }
    {
        NormalTvfFuncCreator creator("abc", {"a", "a"});
        ASSERT_EQ("abc", creator._tvfDef);
        ASSERT_EQ("", creator._name);
        ASSERT_EQ(1, creator._sqlTvfProfileInfos.size());
    }
}

class ErrorTvfFuncCreator : public TvfFuncCreator {
public:
    ErrorTvfFuncCreator() : TvfFuncCreator("") {}
private:
    TvfFunc *createFunction(const HA3_NS(config)::SqlTvfProfileInfo &info) override {
        return nullptr;
    }
    bool initTvfModel(iquan::TvfModel &tvfModel,
                      const HA3_NS(config)::SqlTvfProfileInfo &info) override
    {
        return false;
    }
public:
    bool init(HA3_NS(config)::ResourceReader *resourceReader) override {
        return false;
    }
};

TEST_F(TvfFuncCreatorTest, testInit) {
    NormalTvfFuncCreator creator("");
    ASSERT_TRUE(creator.init(nullptr));
    ErrorTvfFuncCreator errorCreator;
    ASSERT_FALSE(errorCreator.init(nullptr));
}

TEST_F(TvfFuncCreatorTest, testInitTvfModel) {
    NormalTvfFuncCreator creator("");
    iquan::TvfModel tvfModel;
    ASSERT_TRUE(creator.initTvfModel(tvfModel, {}));
}

TEST_F(TvfFuncCreatorTest, testRegTvfModelsGenTvfModelFailed) {
    navi::NaviLoggerProvider provider;
    NormalTvfFuncCreator creator("", {"abc", "abc"});
    iquan::TvfModels tvfModels;
    ASSERT_FALSE(creator.regTvfModels(tvfModels));
    ASSERT_EQ(1, provider.getTrace("generate tvf model [abc] failed").size());
}

TEST_F(TvfFuncCreatorTest, testRegTvfModelsInitTvfModelFailed) {
    navi::NaviLoggerProvider provider;
    ErrorTvfFuncCreator creator;
    creator._tvfDef = tvfDef;
    creator.addTvfFunction({"abc", "abc"});
    iquan::TvfModels tvfModels;
    ASSERT_FALSE(creator.regTvfModels(tvfModels));
    ASSERT_EQ("init tvf model [abc] failed", provider.getErrorMessage());
}

TEST_F(TvfFuncCreatorTest, testRegTvfModelsCheckTvfModelFailed) {
    navi::NaviLoggerProvider provider;
    NormalTvfFuncCreator creator(errorTvfDef, {"abc", "abc"});
    iquan::TvfModels tvfModels;
    ASSERT_FALSE(creator.regTvfModels(tvfModels));
    ASSERT_EQ(1, provider.getTrace("tvf model [abc] invalid").size());
}

TEST_F(TvfFuncCreatorTest, testRegTvfModelsSuccess) {
    NormalTvfFuncCreator creator(tvfDef);
    {
        iquan::TvfModels tvfModels;
        ASSERT_TRUE(creator.regTvfModels(tvfModels));
        ASSERT_EQ(0, tvfModels.functions.size());
    }
    SqlTvfProfileInfo info = {"abc", "abc"};
    creator.addTvfFunction(info);
    {
        iquan::TvfModels tvfModels;
        ASSERT_TRUE(creator.regTvfModels(tvfModels));
        ASSERT_EQ(1, tvfModels.functions.size());
        ASSERT_EQ("abc", tvfModels.functions[0].functionName);
    }
    info = {"def", "abc"};
    creator.addTvfFunction(info);
    {
        iquan::TvfModels tvfModels;
        ASSERT_TRUE(creator.regTvfModels(tvfModels));
        ASSERT_EQ(2, tvfModels.functions.size());
        ASSERT_EQ("abc", tvfModels.functions[0].functionName);
        ASSERT_EQ("def", tvfModels.functions[1].functionName);
    }
}

TEST_F(TvfFuncCreatorTest, testGetSetName) {
    NormalTvfFuncCreator creator("");
    ASSERT_EQ("", creator.getName());
    creator.setName("abc");
    ASSERT_EQ("abc", creator.getName());
}

TEST_F(TvfFuncCreatorTest, testAddTvfFunction) {
    NormalTvfFuncCreator creator("");
    ASSERT_EQ(0, creator._sqlTvfProfileInfos.size());
    SqlTvfProfileInfo info("abc", "abc");
    creator.addTvfFunction(info);
    ASSERT_EQ(1, creator._sqlTvfProfileInfos.size());
    creator.addTvfFunction(info);
    ASSERT_EQ(1, creator._sqlTvfProfileInfos.size());
    info.tvfName = "def";
    creator.addTvfFunction(info);
    ASSERT_EQ(2, creator._sqlTvfProfileInfos.size());
}

TEST_F(TvfFuncCreatorTest, testGetDefaultTvfInfo) {
    {
        SqlTvfProfileInfo info("abc", "abc");
        NormalTvfFuncCreator creator("");
        ASSERT_TRUE(creator.getDefaultTvfInfo().empty());
    }
    {
        SqlTvfProfileInfo info("abc", "abc");
        NormalTvfFuncCreator creator("", info);
        ASSERT_FALSE(creator.getDefaultTvfInfo().empty());
    }
}

TEST_F(TvfFuncCreatorTest, testCheckTvfModel) {
    {
        iquan::TvfModel tvfModel;
        ASSERT_FALSE(TvfFuncCreator::checkTvfModel(tvfModel));
    }
    {
        iquan::TvfModel tvfModel;
        iquan::TvfDef tvfDef;
        iquan::ParamTypeDef paramDef;
        paramDef.type = "int";
        tvfDef.params.scalars.push_back(paramDef);
        tvfModel.functionContent.tvfs.push_back(tvfDef);
        ASSERT_FALSE(TvfFuncCreator::checkTvfModel(tvfModel));
    }
    {
        iquan::TvfModel tvfModel;
        tvfModel.functionContent.tvfs.push_back(iquan::TvfDef());
        ASSERT_TRUE(TvfFuncCreator::checkTvfModel(tvfModel));
    }
    {
        iquan::TvfModel tvfModel;
        iquan::TvfDef tvfDef;
        iquan::ParamTypeDef paramDef;
        paramDef.type = "string";
        tvfDef.params.scalars.push_back(paramDef);
        tvfModel.functionContent.tvfs.push_back(tvfDef);
        ASSERT_TRUE(TvfFuncCreator::checkTvfModel(tvfModel));
    }
}

TEST_F(TvfFuncCreatorTest, testGenerateTvfModel) {
    navi::NaviLoggerProvider provider;
    iquan::TvfModel tvfModel;
    SqlTvfProfileInfo info({"abc", "abc"});
    {
        NormalTvfFuncCreator creator("xxx");
        ASSERT_FALSE(creator.generateTvfModel(tvfModel, info));
        ASSERT_EQ("register tvf models failed [xxx].", provider.getErrorMessage());
    }
    {
        NormalTvfFuncCreator creator(tvfDef);
        ASSERT_TRUE(creator.generateTvfModel(tvfModel, info));
        ASSERT_EQ("abc", tvfModel.functionName);
        ASSERT_EQ("TVF", tvfModel.functionType);
    }
}


END_HA3_NAMESPACE();
