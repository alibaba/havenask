#include <ha3/sql/ops/test/OpTestBase.h>
#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/sql/ops/tvf/TvfFuncCreatorFactory.h"
#include <ha3/sql/ops/tvf/builtin/PrintTableTvfFunc.h>
using namespace std;
using namespace testing;

BEGIN_HA3_NAMESPACE(sql);

class TvfFuncCreatorFactoryTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(sql, TvfFuncCreatorFactoryTest);

void TvfFuncCreatorFactoryTest::setUp() {
}

void TvfFuncCreatorFactoryTest::tearDown() {
}

class MyTvfFuncCreatorFactory : public TvfFuncCreatorFactory {
public:
    bool registerTvfFunctions() override {
        return true;
    }
};

class ErrorTvfFuncCreator : public TvfFuncCreator {
private:
    TvfFunc *createFunction(const HA3_NS(config)::SqlTvfProfileInfo &info) override {
        return nullptr;
    }
    bool initTvfModel(iquan::TvfModel &tvfModel,
                      const HA3_NS(config)::SqlTvfProfileInfo &info) override
    {
        return false;
    }

};

TEST_F(TvfFuncCreatorFactoryTest, testInit) {
    MyTvfFuncCreatorFactory factory;
    KeyValueMap kvMap;
    build_service::config::ResourceReaderPtr reader;
    ASSERT_TRUE(factory.init(kvMap));
    ASSERT_TRUE(factory.init(kvMap, reader));
    ASSERT_TRUE(factory.init(kvMap, reader, nullptr));
}

TEST_F(TvfFuncCreatorFactoryTest, testAddTvfFunctionCreator) {
    {
        MyTvfFuncCreatorFactory factory;
        ASSERT_FALSE(factory.addTvfFunctionCreator(nullptr));
    }
    {
        MyTvfFuncCreatorFactory factory;
        PrintTableTvfFuncCreator *creator = new PrintTableTvfFuncCreator();
        creator->setName("printTableTvf");
        ASSERT_TRUE(factory.addTvfFunctionCreator(creator));
        auto iter = factory._funcCreatorMap.find("printTableTvf");
        ASSERT_TRUE(iter != factory._funcCreatorMap.end());

        ASSERT_TRUE(factory.getTvfFuncCreator("not_exist") == nullptr);
        ASSERT_TRUE(factory.getTvfFuncCreator("printTableTvf") == creator);

        ASSERT_FALSE(factory.addTvfFunctionCreator(creator));
    }
    {
        MyTvfFuncCreatorFactory factory;
        PrintTableTvfFuncCreator *creator = new PrintTableTvfFuncCreator();
        creator->setName("printTableTvf");
        ASSERT_TRUE(factory.addTvfFunctionCreator(creator));
        ASSERT_FALSE(factory.addTvfFunctionCreator(creator));
    }
}

TEST_F(TvfFuncCreatorFactoryTest, testGetTvfFuncCreator) {
    MyTvfFuncCreatorFactory factory;
    PrintTableTvfFuncCreator *creator = new PrintTableTvfFuncCreator();
    factory._funcCreatorMap["abc"] = creator;
    ASSERT_EQ(nullptr, factory.getTvfFuncCreator("not_exist"));
    ASSERT_EQ(creator, factory.getTvfFuncCreator("abc"));
    auto creatorMap = factory.getTvfFuncCreators();
    ASSERT_EQ(1, creatorMap.size());
    ASSERT_EQ(creator, creatorMap["abc"]);
}

END_HA3_NAMESPACE();
