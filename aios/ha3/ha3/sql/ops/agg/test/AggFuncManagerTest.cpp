#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/sql/ops/agg/AggFuncManager.h>
#include <ha3/sql/ops/agg/builtin/CountAggFunc.h>
#include <ha3/sql/ops/agg/AggFuncCreatorFactory.h>
#include <ha3/config/SqlAggPluginConfig.h>

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

class AggFuncManagerTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    AggFuncManagerPtr _aggFuncManager;
    build_service::config::ResourceReaderPtr _resource;
    config::SqlAggPluginConfig _config;
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(agg, AggFuncManagerTest);

void AggFuncManagerTest::setUp() {
    _resource.reset(new build_service::config::ResourceReader(
                    GET_TEST_DATA_PATH() + "/sql_agg_func/"));
    std::string content;
    _resource->getFileContent(content, "sql.json");
    FromJsonString(_config, content);
    _aggFuncManager.reset(new AggFuncManager());
}

void AggFuncManagerTest::tearDown() {
}

TEST_F(AggFuncManagerTest, testInitBuiltinFactory) {
    ASSERT_TRUE(_aggFuncManager->initBuiltinFactory());
    auto func = _aggFuncManager->createAggFunction("COUNT", {}, {}, {"a"}, true);
    ASSERT_TRUE(func != nullptr);
    delete func;
}

TEST_F(AggFuncManagerTest, testInitPluginFactory) {
    ASSERT_TRUE(_aggFuncManager->initPluginFactory(_resource->getConfigPath(), _config));
    auto func = _aggFuncManager->createAggFunction("sum2",
            {matchdoc::ValueTypeHelper<int32_t>::getValueType()}, {"a"}, {"b"}, true);
    ASSERT_TRUE(func != nullptr);
    delete func;
    ASSERT_TRUE(_aggFuncManager->createAggFunction("COUNT", {}, {}, {"a"}, true) == nullptr);
}

TEST_F(AggFuncManagerTest, testInit) {
    ASSERT_TRUE(_aggFuncManager->init(_resource->getConfigPath(), _config));
    ValueType type;
    auto func = _aggFuncManager->createAggFunction("sum2",
            {matchdoc::ValueTypeHelper<int32_t>::getValueType()}, {"a"}, {"b"}, true);
    ASSERT_TRUE(func != nullptr);
    delete func;
    func = _aggFuncManager->createAggFunction("COUNT", {}, {}, {"a"}, true);
    ASSERT_TRUE(func != nullptr);
    delete func;
}

TEST_F(AggFuncManagerTest, testAddFunctions) {
    FackFactory *factory = new FackFactory();
    CountAggFuncCreator *creator = new CountAggFuncCreator();
    ASSERT_TRUE(factory->registerAggFunction("count", creator));
    ASSERT_TRUE(_aggFuncManager->addFunctions(factory));
    ASSERT_EQ(1, _aggFuncManager->_funcToCreator.size());
    ASSERT_EQ(creator, _aggFuncManager->_funcToCreator["count"]);
    ASSERT_FALSE(_aggFuncManager->addFunctions(factory));
    delete factory;
}

TEST_F(AggFuncManagerTest, testCreateAggFunction) {
    FackFactory *factory = new FackFactory();
    CountAggFuncCreator *creator = new CountAggFuncCreator();
    ASSERT_TRUE(factory->registerAggFunction("count", creator));
    ASSERT_TRUE(_aggFuncManager->addFunctions(factory));
    ASSERT_TRUE(_aggFuncManager->createAggFunction("xx", {}, {}, {"a"}, true) == nullptr);
    auto func = _aggFuncManager->createAggFunction("count", {}, {}, {"a"}, true);
    ASSERT_TRUE(func != nullptr);
    delete func;
    delete factory;
}

TEST_F(AggFuncManagerTest, testRegisterFunctionInfos) {
    ASSERT_TRUE(_aggFuncManager->init(_resource->getConfigPath(), _config));
    ASSERT_TRUE(_aggFuncManager->registerFunctionInfos());
    ASSERT_EQ(13, _aggFuncManager->_accSize.size());
    size_t size = 0;
    ASSERT_TRUE(_aggFuncManager->getAccSize("AVG", size));
    ASSERT_EQ(2, size);
    ASSERT_TRUE(_aggFuncManager->getAccSize("sum2", size));
    ASSERT_EQ(1, size);
    ASSERT_FALSE(_aggFuncManager->getAccSize("not_exist", size));
}

END_HA3_NAMESPACE();
