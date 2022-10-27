#include <unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/OptimizerChainManagerCreator.h>
#include <ha3/test/test.h>
#include <suez/turing/common/FileUtil.h>
#include <ha3/common/OptimizerClause.h>
#include <ha3/config/TypeDefine.h>
#include <ha3/search/test/OptimizerChainManagerCreatorTest.h>

using namespace std;
using namespace suez::turing;
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(search);

HA3_LOG_SETUP(search, OptimizerChainManagerCreatorTest);


OptimizerChainManagerCreatorTest::OptimizerChainManagerCreatorTest() {
}

OptimizerChainManagerCreatorTest::~OptimizerChainManagerCreatorTest() {
}

void OptimizerChainManagerCreatorTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void OptimizerChainManagerCreatorTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(OptimizerChainManagerCreatorTest, testSimpleProcess) {
    string confFilePath = TEST_DATA_PATH"/optimizer_config_test/" + CLUSTER_CONFIG_DIR_NAME;
    string bizName = isearch::DEFAULT_BIZ_NAME + isearch::CLUSTER_JSON_FILE_SUFFIX;
    {
        string emptyConfigStr = "";
        OptimizerChainManagerPtr optimizerChainManager = createOptimizerChainManager(emptyConfigStr);
        ASSERT_TRUE(optimizerChainManager != NULL);
        map<std::string, OptimizerPtr> &optimizers = optimizerChainManager->_optimizers;
        ASSERT_EQ(size_t(1), optimizers.size());
        ASSERT_TRUE(optimizers.find("AuxChain") != optimizers.end());
    }
    {
        string configFile = "/no_module/" + bizName;
        OptimizerChainManagerPtr optimizerChainManager = createOptimizerChainManager(
                FileUtil::readFile(confFilePath + configFile));
        ASSERT_TRUE(optimizerChainManager != NULL);
        map<std::string, OptimizerPtr> &optimizers = optimizerChainManager->_optimizers;
        ASSERT_EQ(size_t(1), optimizers.size());
        ASSERT_TRUE(optimizers.find("AuxChain") != optimizers.end());
    }
    {
        string configFile = "/duplicate_module_name.json";
        OptimizerChainManagerPtr optimizerChainManager = createOptimizerChainManager(
                FileUtil::readFile(confFilePath + configFile));
        ASSERT_TRUE(optimizerChainManager == NULL);
    }
    {
        string configFile = "/duplicate_optimizer_name/" + bizName;
        OptimizerChainManagerPtr optimizerChainManager = createOptimizerChainManager(
                FileUtil::readFile(confFilePath + configFile));
        ASSERT_TRUE(optimizerChainManager == NULL);
    }
    {
        string configFile = "/one_module/" + bizName;
        OptimizerChainManagerPtr optimizerChainManager = createOptimizerChainManager(
                FileUtil::readFile(confFilePath + configFile));
        ASSERT_TRUE(optimizerChainManager != NULL);
        map<std::string, OptimizerPtr> &optimizers = optimizerChainManager->_optimizers;
        ASSERT_EQ(size_t(2), optimizers.size());
        ASSERT_TRUE(optimizers.find("AuxChain") != optimizers.end());
        ASSERT_TRUE(optimizers.find("FakeOptimizer") != optimizers.end());
    }
    {
        string configFile = "/two_module/" + bizName;
        OptimizerChainManagerPtr optimizerChainManager = createOptimizerChainManager(
                FileUtil::readFile(confFilePath + configFile));
        ASSERT_TRUE(optimizerChainManager != NULL);
        map<std::string, OptimizerPtr> &optimizers = optimizerChainManager->_optimizers;
        ASSERT_EQ(size_t(3), optimizers.size());
        ASSERT_TRUE(optimizers.find("AuxChain") != optimizers.end());
        ASSERT_TRUE(optimizers.find("test_optimizer1") != optimizers.end());
        ASSERT_TRUE(optimizers.find("test_optimizer2") != optimizers.end());
    }
}

OptimizerChainManagerPtr OptimizerChainManagerCreatorTest::createOptimizerChainManager(
        const string &configStr)
{
    string root = TEST_DATA_PATH"/optimizer_config_test/";
    config::ResourceReaderPtr resourceReader(new config::ResourceReader(root));
    OptimizerChainManagerCreator creator(resourceReader);

    return creator.createFromString(configStr);
}


END_HA3_NAMESPACE(search);

