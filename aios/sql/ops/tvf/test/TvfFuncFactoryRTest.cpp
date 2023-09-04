#include "sql/ops/tvf/TvfFuncFactoryR.h"

#include <iosfwd>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "iquan/common/catalog/TvfFunctionModel.h"
#include "navi/engine/Resource.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/ops/test/OpTestBase.h"
#include "sql/ops/tvf/SqlTvfProfileInfo.h"
#include "sql/ops/tvf/TvfFunc.h"
#include "sql/ops/tvf/TvfFuncCreatorR.h"
#include "sql/ops/tvf/test/MockTvfFunc.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace sql {

class TvfFuncFactoryRTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
};

void TvfFuncFactoryRTest::setUp() {}

void TvfFuncFactoryRTest::tearDown() {}

class ErrorTvfFuncCreator : public TvfFuncCreatorR {
public:
    ErrorTvfFuncCreator()
        : TvfFuncCreatorR("", SqlTvfProfileInfo("factory_error_tvf", "factory_error_fun")) {}

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
const std::string ErrorTvfFuncCreator::RESOURCE_ID = "tvf_test.factory_error_tvf";
REGISTER_RESOURCE(ErrorTvfFuncCreator);

TEST_F(TvfFuncFactoryRTest, testInit) {
    navi::NaviResourceHelper naviRHelper;
    auto factory = naviRHelper.getOrCreateRes<TvfFuncFactoryR>();
    ASSERT_TRUE(factory);
    ASSERT_TRUE(!factory->_tvfFuncCreators.empty());
    ASSERT_TRUE(!factory->_funcToCreator.empty());
    ASSERT_TRUE(!factory->_tvfNameToCreator.empty());
    ASSERT_TRUE(factory->_funcToCreator.end()
                != factory->_funcToCreator.find(ErrorTvfFuncCreator::RESOURCE_ID));
    ASSERT_TRUE(factory->_tvfNameToCreator.end()
                != factory->_tvfNameToCreator.find("factory_error_tvf"));
}

TEST_F(TvfFuncFactoryRTest, test_initFuncCreator) {
    {
        TvfFuncFactoryR factory;
        EXPECT_TRUE(factory.initFuncCreator());
        EXPECT_TRUE(factory._funcToCreator.empty());
    }
    {
        TvfFuncFactoryR factory;
        ErrorTvfFuncCreator errorCreator;
        factory._tvfFuncCreators.insert(&errorCreator);
        EXPECT_TRUE(factory.initFuncCreator());
        EXPECT_FALSE(factory._funcToCreator.empty());
        EXPECT_TRUE(factory._funcToCreator.end()
                    != factory._funcToCreator.find(ErrorTvfFuncCreator::RESOURCE_ID));

        ErrorTvfFuncCreator errorCreator2;
        navi::NaviLoggerProvider provider;
        factory._tvfFuncCreators.insert(&errorCreator2);
        EXPECT_FALSE(factory.initFuncCreator());
        ASSERT_EQ("init tvf function failed: conflict function name[tvf_test.factory_error_tvf].",
                  provider.getErrorMessage());
    }
}

TEST_F(TvfFuncFactoryRTest, test_initTvfInfo) {
    {
        TvfFuncFactoryR factory;
        EXPECT_TRUE(factory.initTvfInfo());
        EXPECT_TRUE(factory._tvfNameToCreator.empty());
    }
    {
        TvfFuncFactoryR factory;
        ErrorTvfFuncCreator errorCreator;
        factory._funcToCreator["tvf_1"] = &errorCreator;
        EXPECT_TRUE(factory.initTvfInfo());
        ASSERT_TRUE(factory._tvfNameToCreator.end()
                    != factory._tvfNameToCreator.find("factory_error_tvf"));

        factory._tvfNameToCreator.clear();
        factory._funcToCreator["tvf_2"] = &errorCreator;

        navi::NaviLoggerProvider provider;
        EXPECT_FALSE(factory.initTvfInfo());
        ASSERT_EQ("add tvf function failed: conflict tvf name[factory_error_tvf].",
                  provider.getErrorMessage());
    }
    {
        TvfFuncFactoryR factory;
        ErrorTvfFuncCreator errorCreator;
        errorCreator._sqlTvfProfileInfos.clear();
        errorCreator._sqlTvfProfileInfos["info"] = SqlTvfProfileInfo("", "func");
        factory._funcToCreator["tvf_1"] = &errorCreator;
        EXPECT_TRUE(factory.initTvfInfo());
        ASSERT_TRUE(factory._tvfNameToCreator.empty());
    }
}

TEST_F(TvfFuncFactoryRTest, test_fillTvfModels) {
    {
        iquan::TvfModels tvfModels;
        TvfFuncFactoryR factory;
        EXPECT_TRUE(factory.fillTvfModels(tvfModels));
        EXPECT_TRUE(tvfModels.functions.empty());
    }
    {
        navi::NaviLoggerProvider provider;
        iquan::TvfModels tvfModels;
        TvfFuncFactoryR factory;
        ErrorTvfFuncCreator errorCreator;
        factory._funcToCreator["tvf_1"] = &errorCreator;
        EXPECT_FALSE(factory.fillTvfModels(tvfModels));
        EXPECT_EQ(0u, tvfModels.functions.size());
        ASSERT_EQ("register tvf models failed [].", provider.getErrorMessage());
    }
    {
        navi::NaviLoggerProvider provider;
        iquan::TvfModels tvfModels;
        TvfFuncFactoryR factory;
        ErrorTvfFuncCreator errorCreator;
        errorCreator._tvfDef = R"tvf_json(
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
        factory._funcToCreator["tvf_1"] = &errorCreator;
        EXPECT_TRUE(factory.fillTvfModels(tvfModels));
        EXPECT_EQ(1u, tvfModels.functions.size());
        ASSERT_EQ("", provider.getErrorMessage());
    }
}

TEST_F(TvfFuncFactoryRTest, test_createTvfFunction) {
    {
        TvfFuncFactoryR factory;
        EXPECT_FALSE(factory.createTvfFunction("abc"));
    }
    {
        TvfFuncFactoryR factory;
        ErrorTvfFuncCreator errorCreator;
        errorCreator._sqlTvfProfileInfos["tvf_1"] = SqlTvfProfileInfo("", "");
        factory._tvfNameToCreator["tvf_1"] = &errorCreator;
        EXPECT_FALSE(factory.createTvfFunction("abc"));
        auto func = factory.createTvfFunction("tvf_1");
        EXPECT_TRUE(func);
        EXPECT_TRUE(dynamic_cast<MockTvfFunc *>(func));
        delete func;
    }
}

} // namespace sql
